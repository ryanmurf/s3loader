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

std::map<Aws::Http::HttpResponseCode, std::string> m = {
        {Aws::Http::HttpResponseCode::REQUEST_NOT_MADE, "REQUEST_NOT_MADE: -1"},
        {Aws::Http::HttpResponseCode::CONTINUE, "CONTINUE: 100"},
        {Aws::Http::HttpResponseCode::SWITCHING_PROTOCOLS, "SWITCHING_PROTOCOLS: 101"},
        {Aws::Http::HttpResponseCode::PROCESSING, "PROCESSING: 102"},
        {Aws::Http::HttpResponseCode::OK, "OK: 200"},
        {Aws::Http::HttpResponseCode::CREATED, "CREATED: 201"},
        {Aws::Http::HttpResponseCode::ACCEPTED, "ACCEPTED: 202"},
        {Aws::Http::HttpResponseCode::NON_AUTHORITATIVE_INFORMATION, "NON_AUTHORITATIVE_INFORMATION: 203"},
        {Aws::Http::HttpResponseCode::NO_CONTENT, "NO_CONTENT: 204"},
        {Aws::Http::HttpResponseCode::RESET_CONTENT, "RESET_CONTENT: 205"},
        {Aws::Http::HttpResponseCode::PARTIAL_CONTENT, "PARTIAL_CONTENT: 206"},
        {Aws::Http::HttpResponseCode::MULTI_STATUS, "MULTI_STATUS: 207"},
        {Aws::Http::HttpResponseCode::ALREADY_REPORTED, "ALREADY_REPORTED: 208"},
        {Aws::Http::HttpResponseCode::IM_USED, "IM_USED: 226"},
        {Aws::Http::HttpResponseCode::MULTIPLE_CHOICES, "MULTIPLE_CHOICES: 300"},
        {Aws::Http::HttpResponseCode::MOVED_PERMANENTLY, "MOVED_PERMANENTLY: 301"},
        {Aws::Http::HttpResponseCode::FOUND, "FOUND: 302"},
        {Aws::Http::HttpResponseCode::SEE_OTHER, "SEE_OTHER: 303"},
        {Aws::Http::HttpResponseCode::NOT_MODIFIED, "NOT_MODIFIED: 304"},
        {Aws::Http::HttpResponseCode::USE_PROXY, "USE_PROXY: 305"},
        {Aws::Http::HttpResponseCode::SWITCH_PROXY, "SWITCH_PROXY: 306"},
        {Aws::Http::HttpResponseCode::TEMPORARY_REDIRECT, "TEMPORARY_REDIRECT: 307"},
        {Aws::Http::HttpResponseCode::PERMANENT_REDIRECT, "PERMANENT_REDIRECT: 308"},
        {Aws::Http::HttpResponseCode::BAD_REQUEST, "BAD_REQUEST: 400"},
        {Aws::Http::HttpResponseCode::UNAUTHORIZED, "UNAUTHORIZED: 401"},
        {Aws::Http::HttpResponseCode::PAYMENT_REQUIRED, "PAYMENT_REQUIRED: 402"},
        {Aws::Http::HttpResponseCode::FORBIDDEN, "FORBIDDEN: 403"},
        {Aws::Http::HttpResponseCode::NOT_FOUND, "NOT_FOUND: 404"},
        {Aws::Http::HttpResponseCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED: 405"},
        {Aws::Http::HttpResponseCode::NOT_ACCEPTABLE, "NOT_ACCEPTABLE: 406"},
        {Aws::Http::HttpResponseCode::PROXY_AUTHENTICATION_REQUIRED, "PROXY_AUTHENTICATION_REQUIRED: 407"},
        {Aws::Http::HttpResponseCode::REQUEST_TIMEOUT, "REQUEST_TIMEOUT: 408"},
        {Aws::Http::HttpResponseCode::CONFLICT, "CONFLICT: 409"},
        {Aws::Http::HttpResponseCode::GONE, "GONE: 410"},
        {Aws::Http::HttpResponseCode::LENGTH_REQUIRED, "LENGTH_REQUIRED: 411"},
        {Aws::Http::HttpResponseCode::PRECONDITION_FAILED, "PRECONDITION_FAILED: 412"},
        {Aws::Http::HttpResponseCode::REQUEST_ENTITY_TOO_LARGE, "REQUEST_ENTITY_TOO_LARGE: 413"},
        {Aws::Http::HttpResponseCode::REQUEST_URI_TOO_LONG, "REQUEST_URI_TOO_LONG: 414"},
        {Aws::Http::HttpResponseCode::UNSUPPORTED_MEDIA_TYPE, "UNSUPPORTED_MEDIA_TYPE: 415"},
        {Aws::Http::HttpResponseCode::REQUESTED_RANGE_NOT_SATISFIABLE, "REQUESTED_RANGE_NOT_SATISFIABLE: 416"},
        {Aws::Http::HttpResponseCode::EXPECTATION_FAILED, "EXPECTATION_FAILED: 417"},
        {Aws::Http::HttpResponseCode::IM_A_TEAPOT, "IM_A_TEAPOT: 418"},
        {Aws::Http::HttpResponseCode::AUTHENTICATION_TIMEOUT, "AUTHENTICATION_TIMEOUT: 419"},
        {Aws::Http::HttpResponseCode::METHOD_FAILURE, "METHOD_FAILURE: 420"},
        {Aws::Http::HttpResponseCode::UNPROC_ENTITY, "UNPROC_ENTITY: 422"},
        {Aws::Http::HttpResponseCode::LOCKED, "LOCKED: 423"},
        {Aws::Http::HttpResponseCode::FAILED_DEPENDENCY, "FAILED_DEPENDENCY: 424"},
        {Aws::Http::HttpResponseCode::UPGRADE_REQUIRED, "UPGRADE_REQUIRED: 426"},
        {Aws::Http::HttpResponseCode::PRECONDITION_REQUIRED, "PRECONDITION_REQUIRED: 427"},
        {Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS, "TOO_MANY_REQUESTS: 429"},
        {Aws::Http::HttpResponseCode::REQUEST_HEADER_FIELDS_TOO_LARGE, "REQUEST_HEADER_FIELDS_TOO_LARGE: 431"},
        {Aws::Http::HttpResponseCode::LOGIN_TIMEOUT, "LOGIN_TIMEOUT: 440"},
        {Aws::Http::HttpResponseCode::NO_RESPONSE, "NO_RESPONSE: 444"},
        {Aws::Http::HttpResponseCode::RETRY_WITH, "RETRY_WITH: 449"},
        {Aws::Http::HttpResponseCode::BLOCKED, "BLOCKED: 450"},
        {Aws::Http::HttpResponseCode::REDIRECT, "REDIRECT: 451"},
        {Aws::Http::HttpResponseCode::REQUEST_HEADER_TOO_LARGE, "REQUEST_HEADER_TOO_LARGE: 494"},
        {Aws::Http::HttpResponseCode::CERT_ERROR, "CERT_ERROR: 495"},
        {Aws::Http::HttpResponseCode::NO_CERT, "NO_CERT: 496"},
        {Aws::Http::HttpResponseCode::HTTP_TO_HTTPS, "HTTP_TO_HTTPS: 497"},
        {Aws::Http::HttpResponseCode::CLIENT_CLOSED_TO_REQUEST, "CLIENT_CLOSED_TO_REQUEST: 499"},
        {Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR: 500"},
        {Aws::Http::HttpResponseCode::NOT_IMPLEMENTED, "NOT_IMPLEMENTED: 501"},
        {Aws::Http::HttpResponseCode::BAD_GATEWAY, "BAD_GATEWAY: 502"},
        {Aws::Http::HttpResponseCode::SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE: 503"},
        {Aws::Http::HttpResponseCode::GATEWAY_TIMEOUT, "GATEWAY_TIMEOUT: 504"},
        {Aws::Http::HttpResponseCode::HTTP_VERSION_NOT_SUPPORTED, "HTTP_VERSION_NOT_SUPPORTED: 505"},
        {Aws::Http::HttpResponseCode::VARIANT_ALSO_NEGOTIATES, "VARIANT_ALSO_NEGOTIATES: 506"},
        {Aws::Http::HttpResponseCode::INSUFFICIENT_STORAGE, "INSUFFICIENT_STORAGE: 506"},
        {Aws::Http::HttpResponseCode::LOOP_DETECTED, "LOOP_DETECTED: 508"},
        {Aws::Http::HttpResponseCode::BANDWIDTH_LIMIT_EXCEEDED, "BANDWIDTH_LIMIT_EXCEEDED: 509"},
        {Aws::Http::HttpResponseCode::NOT_EXTENDED, "NOT_EXTENDED: 510"},
        {Aws::Http::HttpResponseCode::NETWORK_AUTHENTICATION_REQUIRED, "NETWORK_AUTHENTICATION_REQUIRED: 511"},
        {Aws::Http::HttpResponseCode::NETWORK_READ_TIMEOUT, "NETWORK_READ_TIMEOUT: 598"},
        {Aws::Http::HttpResponseCode::NETWORK_CONNECT_TIMEOUT, "NETWORK_CONNECT_TIMEOUT: 599"}};

class S3Source : public UDSource {
private:
    bool verbose = false;
    Aws::SDKOptions options;
    Aws::String bucket_name;
    Aws::String key_name;
    Aws::String sTmpfileName = std::tmpnam(nullptr);

    std::shared_ptr<Aws::Transfer::TransferManager> transferManagerShdPtr;
    std::shared_ptr<Aws::Transfer::TransferHandle> transferHandleShdPtr;

    long retryCount = 0;
    Aws::Transfer::TransferStatus lastStatus;

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
