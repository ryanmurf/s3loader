#include "library.h"
#include "Vertica.h"

#include <iostream>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <fstream>


void hello() {
    Aws::SDKOptions options;

    Aws::InitAPI(options);

    const Aws::String bucket_name = "";
    const Aws::String key_name = "";

    std::cout << "Downloading " << key_name << " from S3 bucket: " <<
              bucket_name << std::endl;

    {
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.region = Aws::Region::US_EAST_1;
        clientConfig.connectTimeoutMs = 30000;
        clientConfig.requestTimeoutMs = 600000;

        Aws::S3::S3Client s3_client(clientConfig);

        Aws::S3::Model::GetObjectRequest object_request;
        object_request.WithBucket(bucket_name).WithKey(key_name);

        auto get_object_outcome = s3_client.GetObject(object_request);

        if (get_object_outcome.IsSuccess()) {
            std::cout << "Downloading now." << std::endl;
            Aws::OFStream local_file;
            local_file.open("/home/ryan/Downloads/" + std::string(key_name.c_str()), std::ios::out | std::ios::binary);
            local_file << get_object_outcome.GetResult().GetBody().rdbuf();
            std::cout << "Done!" << std::endl;
        } else {
            std::cout << "GetObject error: " <<
                      get_object_outcome.GetError().GetExceptionName() << " " <<
                      get_object_outcome.GetError().GetMessage() << std::endl;
        }
    }

    Aws::ShutdownAPI(options);
}