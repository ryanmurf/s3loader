#ifndef S3INDEX_LIBRARY_H
#define S3INDEX_LIBRARY_H

#include "Vertica.h"

#include <stdio.h>
#include <stdlib.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/transfer/TransferManager.h>

using namespace Vertica;

class S3Source : public UDSource {
private:
    bool verbose = false;
    Aws::SDKOptions options;
    Aws::String bucket_name;
    Aws::String key_name;
    Aws::String sTmpfileName = std::tmpnam(nullptr);

    std::shared_ptr<Aws::Transfer::TransferManager> transferManagerShdPtr;
    std::shared_ptr<Aws::Transfer::TransferHandle> transferHandleShdPtr;

    FILE *handle;

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output);
public:
    S3Source(std::string url);

    long totalBytes = 0;
    virtual void setup(ServerInterface &srvInterface);
    virtual void destroy(ServerInterface &srvInterface);
    Aws::String getBucket();
    Aws::String getKey();
};

#endif