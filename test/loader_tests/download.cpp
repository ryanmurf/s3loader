//
// Created by ryan on 7/29/17.
//

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "s3loader.cpp"

class MockVTAllocator : public VTAllocator {
public:
    MockVTAllocator() {

    }
    MOCK_METHOD1(alloc, void*(size_t size));
};

class MockUDFileOperator : Vertica::UDFileOperator {
    MOCK_METHOD2(read, size_t(void *buf, size_t count));
    MOCK_METHOD2(append, size_t(const void *buf, size_t count));
    MOCK_METHOD2(appendWithRetry, void(const void *buffer, size_t size));
    MOCK_METHOD2(seek, off_t(off_t offset, int whence));
    MOCK_CONST_METHOD0(getOffset, off_t());
    MOCK_METHOD5(mmap, void*(void *addr, size_t length, int prot, int flags, off_t offset));
    MOCK_METHOD2(munmap, void(void *addr, size_t length));
    MOCK_METHOD0(fsync, void());
    MOCK_METHOD0(doHurryUp, void());
    MOCK_METHOD0(eof, bool());
};

class MockUDFileSystem : Vertica::UDFileSystem {
    MOCK_CONST_METHOD0(open, UDFileOperator*());
    MOCK_CONST_METHOD4(open, UDFileOperator*(const char *path, int flags, mode_t mode, bool removeOnClose));
    MOCK_CONST_METHOD1(close, void(UDFileOperator *udfo));
    MOCK_CONST_METHOD2(copy, void(const char *srcpath, const char *dstpath));
    MOCK_CONST_METHOD2(statvfs, void(const char *path, struct ::statvfs *buf));
    MOCK_CONST_METHOD2(stat, void(const char *path, struct ::stat *buf));
    MOCK_CONST_METHOD2(mkdirs, void(const char *path, mode_t mode));
    MOCK_CONST_METHOD1(rmdir, void(const char *path));
    MOCK_CONST_METHOD1(rmdirRecursive, void(const char *path));
    MOCK_CONST_METHOD2(listFiles, void(const char *path, std::vector<std::string> &result));
    MOCK_CONST_METHOD2(rename, void(const char *oldpath, const char *newpath));
    MOCK_CONST_METHOD1(remove, void(const char *path));
    MOCK_METHOD2(link, void(const char *oldpath, const char *newpath));
    MOCK_METHOD2(symlink, void(const char *oldpath, const char *newpath));
    MOCK_METHOD0(supportsDirectorySnapshots, bool());
    MOCK_METHOD2(snapshotDirectory, bool(const std::string &path, std::string &snapshot_handle));
    MOCK_METHOD2(restoreSnapshot, bool(const std::string &snapshot_handle, const std::string &path));
    MOCK_METHOD1(deleteSnapshot, bool(const std::string &snapshot_handle));
};

class MockServerImpl : public ServerInterface {
public:
    MockServerImpl(VTAllocator *allocator, LoggingFunc func, const std::string &sqlName, vint udxDebugLogLevel = 0)
            : ServerInterface(allocator, func, sqlName, udxDebugLogLevel) {
    }

    MOCK_METHOD1(getFileSystem, UDFileSystem*(const char *path));
    MOCK_CONST_METHOD1(getFileSystem, UDFileSystem*(const char *pat));
    MOCK_METHOD2(describeFunction, bool(FunctionDescription &func, bool errorIfNotFound));
    MOCK_METHOD3(listTables, void(const RelationDescription &lookup,  std::vector<Oid> &tables, bool errorIfNotFound));
    MOCK_METHOD3(listProjections, void(const RelationDescription &lookup, std::vector<Oid> &projections,  bool errorIfNotFound));
    MOCK_METHOD3(listTableProjections, void(const RelationDescription &baseTable, std::vector<Oid> &projections, bool errorIfNotFound));
    MOCK_METHOD3(listDerivedTables, void(const RelationDescription &baseTable, std::vector<Oid> &tables, bool errorIfNotFound));
    MOCK_METHOD2(describeTable, bool(RelationDescription &baseTable, bool errorIfNotFound));
    MOCK_METHOD2(describeProjection, bool(RelationDescription &proj, bool errorIfNotFound));
    MOCK_METHOD2(describeType, bool(TypeDescription &type, bool errorIfNotFound));
    MOCK_METHOD3(describeBlob, bool(const BlobIdentifier &blobId, BlobDescription &blobDescription, bool errorIfNotFound));
    MOCK_METHOD1(listBlobs, std::vector<BlobDescription>(BlobIdentifier::Namespace nsp));
};

TEST(library, construct) {
    S3Source s3Source("s3://datahub/path/to/s3");
    ASSERT_EQ(std::string(s3Source.getBucket().c_str()), std::string("datahub"));
    ASSERT_EQ(std::string(s3Source.getKey().c_str()), "path/to/s3");
}



TEST(library, download) {
    MockVTAllocator mockVTAllocator;
    ServerInterface::LoggingFunc func;
    MockServerImpl mockServer(&mockVTAllocator, func, std::string("string"), 0);
    S3Source s3Source("");
    s3Source.setup(mockServer);

    DataBuffer dataBuffer;
    LengthBuffer lengthBuffer;
    dataBuffer.buf = new char[10240];
    dataBuffer.offset = 0;
    dataBuffer.size = 10240;

    FILE* file = fopen( "test.bin", "wb" );

    while(s3Source.processWithMetadata(mockServer, dataBuffer, lengthBuffer) == OUTPUT_NEEDED) {
        size_t read = 0;
        while (read != dataBuffer.offset) {
            read = fwrite( dataBuffer.buf + read, sizeof(char), dataBuffer.offset, file );
        }
    }

    delete dataBuffer.buf;
    fclose(file);
    s3Source.destroy(mockServer);
}