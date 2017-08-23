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
    //MOCK_METHOD1(alloc, void*(size_t size));
    void *alloc(size_t size) {
        return std::malloc(size);
    }
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
    MockServerImpl(VTAllocator *allocator, LoggingFunc func, const std::string &sqlName, const ParamReader& paramReader, vint udxDebugLogLevel = 0)
            : ServerInterface(allocator, func, sqlName, paramReader, udxDebugLogLevel) {
    }
    MockServerImpl(VTAllocator *allocator, LoggingFunc func, const std::string &sqlName, const ParamReader& paramReader, ParamReader& sessionParamReader, vint udxDebugLogLevel = 0)
            : ServerInterface(allocator, func, sqlName, paramReader, udxDebugLogLevel) {
        this->udSessionParamReaderMap.addUDSessionParamReader("library", sessionParamReader);
    }
    //MOCK_METHOD0(getParamReader, ParamReader());
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

class MockParamReader: public ParamReader {
public:
    MockParamReader(std::map<std::string, size_t> paramNameToIndex, std::vector<char *> cols, std::vector<VString> svWrappers) {
        this->cols = cols;
        this->paramNameToIndex = paramNameToIndex;
        this->svWrappers = svWrappers;
    }
    //MOCK_METHOD0(getParamNames, std::vector<std::string>());
};

class MockParamWriter: public ParamWriter {
public:
    MockParamWriter(VTAllocator *allocator = NULL) : ParamWriter(allocator) {}


};

class MockNodeSpecifyingPlanContext: public NodeSpecifyingPlanContext {
public:
    MockNodeSpecifyingPlanContext(ParamWriter& writer, std::vector<std::string> clusterNodes, bool canApportion)
            : NodeSpecifyingPlanContext(writer, clusterNodes, canApportion) {}
    //MOCK_CONST_METHOD0(canApportionSource, bool());
    //MOCK_CONST_METHOD0(getTargetNodes, std::vector<std::string>&());
    //MOCK_METHOD1(setTargetNodes, void(const std::vector<std::string>& nodes));
};

TEST(library, construct) {
    S3Source s3Source("s3://datahub/path/to/s3");
    ASSERT_EQ(std::string(s3Source.getBucket().c_str()), std::string("datahub"));
    ASSERT_EQ(std::string(s3Source.getKey().c_str()), "path/to/s3");
}


TEST(library, download) {
    //id
    char id[] = "";
    char secret[] = "";
    char endpoint[] = "";
    char tempfile[] = "";
    std::string s3uri = "";
    EE::StringValue *sId = (EE::StringValue *)malloc(sizeof(EE::StringValue) + sizeof(id) * sizeof(char) + 1);
    sId->slen = sizeof(id) - 1;
    sId->sloc = 0;
    std::memcpy(&sId->base, &id[0], sizeof(id));
    //secret

    EE::StringValue *sSecret = (EE::StringValue *)malloc(sizeof(EE::StringValue) + sizeof(secret) * sizeof(char) + 1);
    sSecret->slen = sizeof(secret) - 1;
    sSecret->sloc = 0;
    std::memcpy(&sSecret->base, &secret[0], sizeof(secret));
    //verbose
    vbool bVerbose = true;

    //endpoint

    EE::StringValue *sEndpoint = (EE::StringValue *)malloc(sizeof(EE::StringValue) + sizeof(endpoint) * sizeof(char) + 1);
    sEndpoint->slen = sizeof(endpoint) - 1;
    sEndpoint->sloc = 0;
    std::memcpy(&sEndpoint->base, &endpoint[0], sizeof(endpoint));

    std::map<std::string, size_t> paramNameToIndex;
    paramNameToIndex["aws_id"] = 0;
    paramNameToIndex["aws_secret"] = 1;
    paramNameToIndex["aws_verbose"] = 2;
    paramNameToIndex["aws_endpoint"] = 3;

    std::vector<VString> svWrappers;
    svWrappers.push_back(VString(NULL, NULL, StringNull));
    svWrappers.push_back(VString(NULL, NULL, StringNull));
    svWrappers.push_back(VString(NULL, NULL, StringNull));
    svWrappers.push_back(VString(NULL, NULL, StringNull));

    std::vector<char *> cols;
    cols.push_back(reinterpret_cast<char*>(sId));
    cols.push_back(reinterpret_cast<char*>(sSecret));
    cols.push_back(reinterpret_cast<char*>(&bVerbose));
    cols.push_back(reinterpret_cast<char*>(sEndpoint));
    MockParamReader mockParamReader(paramNameToIndex,cols,svWrappers);

    MockVTAllocator mockVTAllocator;
    ServerInterface::LoggingFunc func;
    MockServerImpl mockServer(&mockVTAllocator, func, std::string("string"), mockParamReader, mockParamReader, 0);
    S3Source s3Source(s3uri);
    s3Source.setup(mockServer);

    DataBuffer dataBuffer;
    LengthBuffer lengthBuffer;
    dataBuffer.buf = new char[10240];
    dataBuffer.offset = 0;
    dataBuffer.size = 10240;

    FILE* file = fopen(tempfile, "wb" );
    long total = 0;
    while(s3Source.processWithMetadata(mockServer, dataBuffer, lengthBuffer) != DONE) {
        if (dataBuffer.offset > 0) {
            total += fwrite(dataBuffer.buf, sizeof(char), dataBuffer.offset, file);
            dataBuffer.offset = 0;
        }
    }

    total += fwrite(dataBuffer.buf, sizeof(char), dataBuffer.offset, file);
    std::cout << total << " Bytes written to file\n" << "Cleanup" << std::endl;
    GTEST_ASSERT_EQ(total, s3Source.totalBytes);
    delete dataBuffer.buf;
    fclose(file);
    s3Source.destroy(mockServer);
}

TEST(library, plan) {
    MockVTAllocator mockVTAllocator;
    ServerInterface::LoggingFunc func;

    std::map<std::string, size_t> paramNameToIndex;
    paramNameToIndex["url"] = 1;
    std::vector<char *> cols;

    std::vector<VString> svWrappers;
    svWrappers.push_back(VString(NULL, NULL, StringNull));
    svWrappers.push_back(VString(NULL, NULL, StringNull));

    char temp[] = "s3://bucket/path1|s3://bucket/path2";
    EE::StringValue *val = (EE::StringValue *)malloc(sizeof(EE::StringValue) + 36 * sizeof(char));
    val->slen = 35;
    val->sloc = 0;
    std::memcpy(&val->base, &temp[0], sizeof(temp));
    char temp1[] = "";
    cols.push_back(&temp1[0]);
    cols.push_back(reinterpret_cast<char*>(val));
    MockParamReader mockParamReader(paramNameToIndex,cols,svWrappers);
    MockServerImpl mockServer(&mockVTAllocator, func, std::string("string"), mockParamReader, 0);

    MockParamWriter mockParamWriter(&mockVTAllocator);

    std::vector<std::string> clusterNodes;
    clusterNodes.push_back("node_a");
    clusterNodes.push_back("node_b");

    MockNodeSpecifyingPlanContext mockNodeSpecifyingPlanContext(mockParamWriter, clusterNodes, true);

    std::cout << "Starting test" << std::endl;
    S3SourceFactory s3SourceFactory;
    s3SourceFactory.plan(mockServer, mockNodeSpecifyingPlanContext);

    GTEST_ASSERT_EQ(mockNodeSpecifyingPlanContext.getTargetNodes().size(), 2);
    GTEST_ASSERT_EQ(mockNodeSpecifyingPlanContext.getWriter().getParamNames().size(), 2);
    GTEST_ASSERT_EQ(mockNodeSpecifyingPlanContext.getWriter().getStringRef("node_a:0").str(), "s3://bucket/path1");
    GTEST_ASSERT_EQ(mockNodeSpecifyingPlanContext.getWriter().getStringRef("node_b:1").str(), "s3://bucket/path2");
}