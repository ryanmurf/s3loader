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
    Aws::ShutdownAPI(options);
}

Aws::String S3Source::getBucket() {
    return bucket_name;
}

Aws::String S3Source::getKey() {
    return key_name;
}


class S3SourceFactory : public SourceFactory {
public:

    virtual void plan(ServerInterface &srvInterface,
                      NodeSpecifyingPlanContext &planCtx) {
        std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        /* Check parameters */
        if (args.size() != 1 || find(args.begin(), args.end(), "url") == args.end()) {
            vt_report_error(0, "Must have exactly one argument, 'url'");
        }

        Vertica::ParamWriter &pwriter = planCtx.getWriter();

        const std::vector<std::string>& nodes = planCtx.getTargetNodes();

        // Keep track of which nodes actually have something useful to do
        std::set<size_t> usedNodes;

        // Get the files that the source has
        std::vector<std::string> files;
        std::string s = srvInterface.getParamReader().getStringRef("url").str();
        std::string delimiter = "|";
        size_t pos = 0;
        std::string token;
        while ((pos = s.find(delimiter)) != std::string::npos) {
            token = s.substr(0, pos);
            files.push_back(token);
            s.erase(0, pos + delimiter.length());
        }
        files.push_back(s);

        // Assign the files to the nodes in round robin fashion
        size_t nodeIdx = 0;
        std::stringstream ss;
        for (size_t i=0; i<files.size(); ++i) {
            // Param named nodename:i, value is file name
            ss.str("");
            ss << nodes[nodeIdx] << ":" << i;
            const std::string fieldName = ss.str();

            // Set the appropriate field
            pwriter.getStringRef(fieldName).copy(files[i]);

            // select next node, wrapping as necessary
            usedNodes.insert(nodeIdx);
            nodeIdx = (nodeIdx+1) % nodes.size();
        }

        // Set which nodes should be used
        std::vector<std::string> usedNodesStr;
        for (std::set<size_t>::iterator it = usedNodes.begin(); it != usedNodes.end(); ++it)  {
            usedNodesStr.push_back(nodes[*it]);
        }
        planCtx.setTargetNodes(usedNodesStr);
    }


    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
                                                    NodeSpecifyingPlanContext &planCtx) {

        const std::string nodeName = srvInterface.getCurrentNodeName();

        std::vector<Vertica::UDSource*> retVal;

        // Find all the files destined for this node
        Vertica::ParamReader &preader = planCtx.getReader();

        std::vector<std::string> paramNames = preader.getParamNames();
        for (std::size_t i=0; i<paramNames.size(); ++i)
        {
            const std::string &paramName = paramNames[i];

            // if the param name starts with this node name, get the value
            // (which is a filename) and make a new source for reading that file
            size_t pos = paramName.find(nodeName);
            if (pos == 0) {
                std::string s3path = preader.getStringRef(paramName).str();
                retVal.push_back(Vertica::vt_createFuncObject<S3Source>(srvInterface.allocator, s3path));
            }
        }

        return retVal;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addLongVarchar(30000000, "url");
    }
};
RegisterFactory(S3SourceFactory);