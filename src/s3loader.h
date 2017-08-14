#ifndef S3INDEX_LIBRARY_H
#define S3INDEX_LIBRARY_H

#include "Vertica.h"
#include <aws/s3/S3Client.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

using namespace Vertica;

class S3Source : public UDSource {
private:
    bool verbose = false;
    Aws::SDKOptions options;
    Aws::String bucket_name;
    Aws::String key_name;

    Aws::S3::S3Client* s3_client;
    Aws::S3::Model::GetObjectResult result;
    Aws::StringStream* stream;

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output);
public:
    S3Source(std::string url);

    virtual void setup(ServerInterface &srvInterface);
    virtual void destroy(ServerInterface &srvInterface);
    Aws::String getBucket();
    Aws::String getKey();
};

#endif