#include "s3loader.h"

#include <aws/s3/model/GetObjectRequest.h>
#include <fstream>

using namespace Vertica;

S3Source::S3Source(std::string url) {
    std::stringstream path;
    path << url.substr(5, url.size());

    std::string segment;
    std::vector<std::string> seglist;

    std::getline(path, segment, '/');
    bucket_name = Aws::Utils::StringUtils::to_string(segment);
    std::string sPath = path.str();
    key_name = Aws::Utils::StringUtils::to_string(sPath.substr(bucket_name.size()+1, sPath.size()));
}

StreamState S3Source::process(ServerInterface &srvInterface, DataBuffer &output) {
    output.offset = result.GetBody().readsome(output.buf, output.size);
    if (result.GetBody().eof() || result.GetBody().bad() || output.offset == 0) {
        return DONE;
    }
    return OUTPUT_NEEDED;
}

void S3Source::setup(ServerInterface &srvInterface) {
    std::cout << "Setting up API" << std::endl;
    Aws::InitAPI(options);

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    clientConfig.region = Aws::Region::US_EAST_1;
    clientConfig.connectTimeoutMs = 5000;
    clientConfig.requestTimeoutMs = 15000;

    std::cout << "Initializing client" << std::endl;
    Aws::S3::S3Client client = Aws::S3::S3Client(clientConfig);

    s3_client = &client;
    Aws::S3::Model::GetObjectRequest object_request;
    object_request.WithBucket(bucket_name).WithKey(key_name);

    stream = Aws::New<Aws::StringStream>("s3buffer");
    object_request.SetResponseStreamFactory([this]() {
        return stream;
    });

    std::cout << "Making call" << std::endl;
    Aws::S3::Model::GetObjectOutcome get_object_outcome = s3_client->GetObject(object_request);

    if (get_object_outcome.IsSuccess()) {
        std::cout << "success, getting body" << std::endl;
        result = get_object_outcome.GetResultWithOwnership();
        stream->seekg(0, std::ios::end);
        int size = stream->tellg();
        std::cout << "Size of body " << size << " Size of Content " << get_object_outcome.GetResult().GetContentLength() << " tell G " << result.GetBody().tellg() << std::endl;
        stream->seekg(0, std::ios::beg);
    } else {
        std::cout << "GetObject error: " <<
                  get_object_outcome.GetError().GetExceptionName() << " " <<
                  get_object_outcome.GetError().GetMessage() << std::endl;
    }
}

void S3Source::destroy(ServerInterface &srvInterface) {
    //delete[] buffer;
    Aws::ShutdownAPI(options);
}

Aws::String S3Source::getBucket() {
    return bucket_name;
}

Aws::String S3Source::getKey() {
    return key_name;
}


class s3SourceFactory : public SourceFactory {
public:

    virtual void plan(ServerInterface &srvInterface,
                      NodeSpecifyingPlanContext &planCtxt) {
        std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        /* Check parameters */
        if (args.size() != 1 || find(args.begin(), args.end(), "url") == args.end()) {
            vt_report_error(0, "Must have exactly one argument, 'url'");
        }

        /* Populate planData */
        planCtxt.getWriter().getLongStringRef("url").copy(srvInterface.getParamReader().getStringRef("url"));

        /* Munge nodes list */
        std::vector<std::string> executionNodes = planCtxt.getClusterNodes();
        while (executionNodes.size() > 1) executionNodes.pop_back();  // Only run on one node.  Don't care which.
        planCtxt.setTargetNodes(executionNodes);
    }


    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
                                                    NodeSpecifyingPlanContext &planCtxt) {
        std::vector<UDSource*> retVal;
        retVal.push_back(vt_createFuncObject<S3Source>(srvInterface.allocator,
                                                         planCtxt.getReader().getStringRef("url").str()));
        return retVal;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addLongVarchar(30000000, "url");
    }
};
RegisterFactory(s3SourceFactory);