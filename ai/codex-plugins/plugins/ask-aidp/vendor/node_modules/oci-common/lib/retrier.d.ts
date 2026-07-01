/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import { WaiterConfigurationDetails } from "./waiter";
import { HttpClient } from "./http";
import { HttpRequest } from "./http-request";
import { OciError } from "..";
import { Logger } from "./log";
/**
 * This class implements the retrier
 * NOTE : Retries are not supported for requests that have binary or stream bodies
 * this also affects UploadManager operations
 * For all requests with binary/stream bodies, retry attempts will be made if RetryConfigurationDetails.backupBinaryBody
 * is set to true, or if the original stream body is able to be retried
 *
 */
export declare type RetryConfiguration = Partial<RetryConfigurationDetails>;
export interface RetryConfigurationDetails extends WaiterConfigurationDetails {
    retryCondition: (response: OciError) => boolean;
    backupBinaryBody: boolean;
}
export declare class DefaultRetryCondition {
    /**
     * Default retry condition for Retry mechanism
     * NOTE : Retries are not supported for requests that have binary or stream bodies
     */
    private static RETRYABLE_SERVICE_ERRORS;
    static shouldBeRetried(error: OciError): boolean;
}
export declare const NoRetryConfigurationDetails: RetryConfigurationDetails;
export declare const OciSdkDefaultRetryConfiguration: RetryConfigurationDetails;
export declare class GenericRetrier {
    private _retryConfiguration;
    private _logger;
    private static OPC_CLIENT_RETRIES_HEADER;
    private static OCI_SDK_DEFAULT_RETRY_ENABLED;
    constructor(retryConfiguration: RetryConfiguration);
    static get envVariableCheckForDefaultRetry(): RetryConfiguration | null;
    private static DefaultRetryConfiguration;
    static get defaultRetryConfiguration(): RetryConfiguration | null;
    static set defaultRetryConfiguration(retryConfig: RetryConfiguration | null);
    set logger(logger: Logger);
    get logger(): Logger;
    get backUpBinaryBody(): boolean;
    get retryConfiguration(): RetryConfigurationDetails;
    static createPreferredRetrier(clientRetryConfiguration?: RetryConfiguration, requestRetryConfiguration?: RetryConfiguration, specRetryConfiguration?: RetryConfiguration): GenericRetrier;
    makeServiceCall(httpClient: HttpClient, request: HttpRequest, targetService: string, operationName: string, apiReferenceLink: string, excludeBody?: boolean): Promise<Response>;
    private static refreshRequest;
    private addOpcClientRetryHeader;
    private static isRequestRetryable;
    private static isRetryableStream;
}
