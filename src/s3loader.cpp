#include "s3loader.h"



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
    auto status = transferHandleShdPtr.get()->GetStatus();
    if (status == Aws::Transfer::TransferStatus::NOT_STARTED ||
            status == Aws::Transfer::TransferStatus::IN_PROGRESS) {
        return KEEP_GOING;
    } else if (status == Aws::Transfer::TransferStatus::COMPLETED) {
        if (handle == NULL) {
            if (verbose) {
                std::cout << "Opening File " << this->sTmpfileName << std::endl;
            }
            handle = std::fopen(this->sTmpfileName.c_str(), "r");
            if (handle == NULL) {
                vt_report_error(0, "Error opening file [%s]", this->sTmpfileName.c_str());
            }
            fseek (handle , 0 , SEEK_END);
            long fileSize = ftell (handle);
            if (verbose) {
                std::cout << "Size of Download " << fileSize << std::endl;
            }
            rewind (handle);
            if (transferHandleShdPtr.get()->GetBytesTotalSize() != transferHandleShdPtr.get()->GetBytesTransferred() ||
                transferHandleShdPtr.get()->GetBytesTransferred() != fileSize) {
                vt_report_error(0,
                                "Bytes reported by s3 [%u] doesn't match bytes read from s3 [%u] or doesn't match file size of [%u]",
                                transferHandleShdPtr.get()->GetBytesTotalSize(),
                                transferHandleShdPtr.get()->GetBytesTransferred(), fileSize);
            }
        }
        output.offset += fread(output.buf + output.offset, 1, output.size - output.offset, handle);
        this->totalBytes += output.offset;
        if (feof(handle)) {
            return DONE;
        } else {
            return OUTPUT_NEEDED;
        }
    } else if (status == Aws::Transfer::TransferStatus::FAILED && lastStatus != Aws::Transfer::TransferStatus::FAILED) {
        std::cout << "FAILED Try " << retryCount << " RESTARTING" << std::endl;
        std::cout << "Status Code: " << static_cast<int >(transferHandleShdPtr.get()->GetLastError().GetResponseCode()) << std::endl;
        std::cout << transferHandleShdPtr.get()->GetLastError().GetExceptionName() << std::endl;
        std::cout << transferHandleShdPtr.get()->GetLastError().GetMessage() << std::endl;
        if (retryCount < 4) {
            transferManagerShdPtr.get()->RetryDownload(transferHandleShdPtr);
            retryCount++;
            return KEEP_GOING;
        } else {
            return DONE;
        }
    } else if (status == Aws::Transfer::TransferStatus::ABORTED ||
            status == Aws::Transfer::TransferStatus::CANCELED) {
        std::cout << "ABORTED or CANCELED RESTARTING" << std::endl;
        return DONE;
    }
    lastStatus = status;
    return OUTPUT_NEEDED;
}

void S3Source::setup(ServerInterface &srvInterface) {
    ParamReader pSessionParams = srvInterface.getUDSessionParamReader("library");
    std::string id = pSessionParams.containsParameter("aws_id") ?
                     pSessionParams.getStringRef("aws_id").str() : "";
    std::string secret = pSessionParams.containsParameter("aws_secret") ?
                         pSessionParams.getStringRef("aws_secret").str() : "";
    verbose = pSessionParams.containsParameter("aws_verbose") ?
              pSessionParams.getBoolRef("aws_verbose") : false;
    std::string endpoint = pSessionParams.containsParameter("aws_endpoint") ?
                           pSessionParams.getStringRef("aws_endpoint").str() : "";

    if (verbose) {
        std::cout << "Setting up API for " << key_name << std::endl;
    }

    Aws::InitAPI(options);

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.scheme = Aws::Http::Scheme::HTTPS;
    clientConfig.region = Aws::Region::US_EAST_1;
    clientConfig.connectTimeoutMs = 5000;
    clientConfig.requestTimeoutMs = 15000;
    if (!endpoint.empty()) {
        clientConfig.endpointOverride = Aws::Utils::StringUtils::to_string(endpoint);
    }

    if (verbose) {
        std::cout << "Initializing client for " << key_name << std::endl;
    }

    Aws::Auth::AWSCredentials credentials(Aws::Utils::StringUtils::to_string(id),
                                          Aws::Utils::StringUtils::to_string(secret));
    auto s3Client = (!id.empty() && !secret.empty()) ? Aws::MakeShared<Aws::S3::S3Client>("main", credentials,
                                                                                          clientConfig)
                                                     : Aws::MakeShared<Aws::S3::S3Client>("main", clientConfig);
    Aws::Transfer::TransferManagerConfiguration transferConfig;
    transferConfig.s3Client = s3Client;

    if (verbose) {
        std::cout << "Initializing transfer client for " << key_name << std::endl;
    }

    transferManagerShdPtr = Aws::Transfer::TransferManager::Create(transferConfig);
    Aws::Transfer::DownloadConfiguration downloadConfiguration;

    //Using a file is going to be slow. Need to use CreateDownloadStreamCallback. Need it to use a piped buffer stream,
    //so that the manager can write to it (different thread) and the main thread can load it to vertica. The read and
    //write need to be thread safe and block until bytes are available. Early attempts at this have resulted in failure
    //because as soon as the transfer completes the stringstream becomes corrupted.
    transferHandleShdPtr = transferManagerShdPtr.get()->DownloadFile(this->bucket_name, this->key_name,
                                                                     this->sTmpfileName,
                                                                     downloadConfiguration);
    lastStatus = transferHandleShdPtr.get()->GetStatus();
    handle = NULL;
    if (verbose) {
        std::cout << "Started transfer for " << key_name << std::endl;
    }

}

void S3Source::destroy(ServerInterface &srvInterface) {
    Aws::ShutdownAPI(options);
    fclose(handle);
    remove(this->sTmpfileName.c_str());
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