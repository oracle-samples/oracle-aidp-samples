"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.GenericRetrier = exports.OciSdkDefaultRetryConfiguration = exports.NoRetryConfigurationDetails = exports.DefaultRetryCondition = void 0;
const waiter_1 = require("./waiter");
const helper_1 = require("./helper");
const __1 = require("..");
const constants_1 = require("./constants");
const TROUBLESHOOT_URL = "https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_troubleshooting.htm";
class DefaultRetryCondition {
    static shouldBeRetried(error) {
        return (error.statusCode === 500 ||
            error.statusCode === 502 ||
            error.statusCode === 503 ||
            error.statusCode === 504 ||
            error.statusCode == -1 ||
            isNaN(error.statusCode) || // no StatusCode means client side error. These are considered retryable.
            (DefaultRetryCondition.RETRYABLE_SERVICE_ERRORS.has(error.statusCode) &&
                DefaultRetryCondition.RETRYABLE_SERVICE_ERRORS.get(error.statusCode) === error.serviceCode));
    }
}
exports.DefaultRetryCondition = DefaultRetryCondition;
/**
 * Default retry condition for Retry mechanism
 * NOTE : Retries are not supported for requests that have binary or stream bodies
 */
DefaultRetryCondition.RETRYABLE_SERVICE_ERRORS = new Map([
    [409, "IncorrectState"],
    [429, "TooManyRequests"]
]);
const NO_RETRY_MAXIMUM_NUMBER_OF_ATTEMPTS = 1;
const NO_RETRY_MAXIMUM_DELAY_IN_SECONDS = 30;
const OCI_SDK_DEFAULT_RETRY_MAXIMUM_NUMBER_OF_ATTEMPTS = 8;
const OCI_SDK_DEFAULT_RETRY_MAXIMUM_DELAY_IN_SECONDS = 30;
const CONTENT_TYPE_HEADER = "content-type";
const SERVER_SIDE_EVENT_TEXT_STREAM = "text/event-stream";
exports.NoRetryConfigurationDetails = {
    terminationStrategy: new waiter_1.MaxAttemptsTerminationStrategy(NO_RETRY_MAXIMUM_NUMBER_OF_ATTEMPTS),
    delayStrategy: new waiter_1.ExponentialBackoffDelayStrategyWithJitter(NO_RETRY_MAXIMUM_DELAY_IN_SECONDS),
    retryCondition: DefaultRetryCondition.shouldBeRetried,
    backupBinaryBody: false
};
exports.OciSdkDefaultRetryConfiguration = {
    terminationStrategy: new waiter_1.MaxAttemptsTerminationStrategy(OCI_SDK_DEFAULT_RETRY_MAXIMUM_NUMBER_OF_ATTEMPTS),
    delayStrategy: new waiter_1.ExponentialBackoffDelayStrategyWithJitter(OCI_SDK_DEFAULT_RETRY_MAXIMUM_DELAY_IN_SECONDS),
    retryCondition: DefaultRetryCondition.shouldBeRetried,
    backupBinaryBody: false
};
class GenericRetrier {
    constructor(retryConfiguration) {
        this._logger = undefined;
        const preferredRetryConfig = Object.assign(Object.assign({}, exports.NoRetryConfigurationDetails), retryConfiguration);
        this._retryConfiguration = preferredRetryConfig;
    }
    static get envVariableCheckForDefaultRetry() {
        if (process.env[GenericRetrier.OCI_SDK_DEFAULT_RETRY_ENABLED] === constants_1.BooleanString.FALSE) {
            return exports.NoRetryConfigurationDetails;
        }
        else if (process.env[GenericRetrier.OCI_SDK_DEFAULT_RETRY_ENABLED] === constants_1.BooleanString.TRUE) {
            return exports.OciSdkDefaultRetryConfiguration;
        }
        else
            return null;
    }
    static get defaultRetryConfiguration() {
        return GenericRetrier.DefaultRetryConfiguration;
    }
    static set defaultRetryConfiguration(retryConfig) {
        GenericRetrier.DefaultRetryConfiguration = Object.assign(Object.assign({}, GenericRetrier.DefaultRetryConfiguration), retryConfig);
    }
    set logger(logger) {
        this._logger = logger;
    }
    get logger() {
        return this._logger;
    }
    get backUpBinaryBody() {
        return this.retryConfiguration.backupBinaryBody;
    }
    get retryConfiguration() {
        return this._retryConfiguration;
    }
    static createPreferredRetrier(clientRetryConfiguration, requestRetryConfiguration, specRetryConfiguration) {
        let retryConfigToUse = [
            requestRetryConfiguration,
            clientRetryConfiguration,
            GenericRetrier.defaultRetryConfiguration
        ].filter(configuration => configuration !== null && configuration !== undefined)[0];
        retryConfigToUse = Object.assign(Object.assign({}, specRetryConfiguration), retryConfigToUse);
        return new GenericRetrier(retryConfigToUse);
    }
    makeServiceCall(httpClient, request, targetService, operationName, apiReferenceLink, excludeBody) {
        return __awaiter(this, void 0, void 0, function* () {
            const waitContext = new waiter_1.WaitContextImpl();
            let lastKnownError;
            let shouldBeRetried = true;
            const timestamp = new Date();
            const endpoint = `${request.method} ${request.uri}`;
            let attempt = 0;
            const configuration = this.retryConfiguration;
            let maxDelayInSeconds = undefined;
            let maxAttempts = undefined;
            if (configuration.delayStrategy && configuration.delayStrategy.maxDelayInSeconds) {
                maxDelayInSeconds = configuration.delayStrategy.maxDelayInSeconds;
            }
            if (configuration.terminationStrategy && configuration.terminationStrategy.maxAttempts) {
                maxAttempts = configuration.terminationStrategy.maxAttempts;
            }
            if (this.logger) {
                this.logger.debug(`Retry policy to use: MaximumNumberAttempts=${maxAttempts}, MaxSleepBetween=${maxDelayInSeconds}, ExponentialBackoffBase=2`);
            }
            while (true) {
                try {
                    this.addOpcClientRetryHeader(request);
                    const response = yield httpClient.send(request, excludeBody, targetService, operationName, timestamp.toISOString(), endpoint, apiReferenceLink);
                    if (response.status && response.status >= 200 && response.status <= 299) {
                        const currentTime = new Date().getTime();
                        const timeElapsed = currentTime - timestamp.getTime();
                        if (this.logger) {
                            this.logger.debug(`Total Latency for this API call is: ${timeElapsed} ms`);
                        }
                        return response;
                    }
                    else if (response.code === "EOPENBREAKER") {
                        // Circuit Breaker is in OPEN state
                        const circuitBreakerError = response;
                        const errorObject = new __1.OciError(circuitBreakerError.code, "unknown code", circuitBreakerError.message, "unknown", targetService, operationName, timestamp.toISOString(), endpoint, apiReferenceLink);
                        shouldBeRetried = this.retryConfiguration.retryCondition(errorObject); // TODO: need retryCondition to accept errorObject coming from Circuit Breaker
                        lastKnownError = errorObject;
                    }
                    else {
                        const errBody = yield helper_1.handleErrorBody(response);
                        const errorObject = helper_1.handleErrorResponse(response, errBody, targetService, operationName, timestamp.toISOString(), endpoint, apiReferenceLink);
                        shouldBeRetried = this.retryConfiguration.retryCondition(errorObject);
                        lastKnownError = errorObject;
                    }
                }
                catch (err) {
                    // These are non-service errors
                    shouldBeRetried =
                        err !== undefined && err.shouldBeRetried !== undefined ? err.shouldBeRetried : true;
                    lastKnownError = {
                        code: err.code,
                        message: err.message,
                        requestEndpoint: endpoint,
                        troubleshootingPage: `See ${TROUBLESHOOT_URL} for help troubleshooting this error, or contact support and provide this full error message.`
                    };
                }
                let currentTime = new Date().getTime();
                let timeElapsed = currentTime - timestamp.getTime();
                if (!shouldBeRetried || !GenericRetrier.isRequestRetryable(request)) {
                    console.warn(`Request cannot be retried. Not Retrying. Exception occurred : ${lastKnownError}`);
                    if (this.logger) {
                        this.logger.debug(`Total Latency for this API call is: ${timeElapsed} ms`);
                    }
                    throw lastKnownError;
                }
                else if (this.retryConfiguration.terminationStrategy.shouldTerminate(waitContext)) {
                    console.warn(`All retry attempts have exhausted. Total Attempts : ${waitContext.attemptCount +
                        1}. Last exception occurred : ${lastKnownError}`);
                    if (this.logger) {
                        this.logger.debug(`Total Latency for this API call is: ${timeElapsed} ms`);
                    }
                    throw lastKnownError;
                }
                const delayTime = this.retryConfiguration.delayStrategy.delay(waitContext);
                waitContext.attemptCount++;
                console.warn(`Request failed with Exception : ${lastKnownError}\nRetrying request -> Total Attempts : ${waitContext.attemptCount}, Retrying after ${delayTime} seconds...`);
                yield waiter_1.delay(delayTime);
                GenericRetrier.refreshRequest(request);
                attempt += 1;
                if (this.logger) {
                    lastKnownError instanceof __1.OciError
                        ? this.logger.debug(`Http Status Code: ${lastKnownError.statusCode}, Error Code: ${lastKnownError.serviceCode}, Attempt: ${attempt}`)
                        : this.logger.debug(`Code: ${lastKnownError.code}, Message: ${lastKnownError.message}, Attempt: ${attempt}`);
                }
            }
        });
    }
    static refreshRequest(request) {
        request.headers.set("x-date", new Date().toUTCString());
    }
    addOpcClientRetryHeader(request) {
        const terminationStrategy = this.retryConfiguration.terminationStrategy;
        const opcClientRetryHeader = request.headers.get(GenericRetrier.OPC_CLIENT_RETRIES_HEADER);
        if (terminationStrategy instanceof waiter_1.MaxAttemptsTerminationStrategy &&
            terminationStrategy.maxAttempts > 1 &&
            (opcClientRetryHeader === undefined || opcClientRetryHeader === null)) {
            request.headers.set(GenericRetrier.OPC_CLIENT_RETRIES_HEADER, "true");
        }
    }
    static isRequestRetryable(request) {
        if (!request.body)
            return true;
        else if (request.body) {
            return this.isRetryableStream(request.body);
        }
    }
    static isRetryableStream(obj) {
        if (obj instanceof Uint8Array || obj instanceof Buffer || typeof obj === "string") {
            return true;
        }
        //Node.JS's Readable, JavaScript's ReadableStream & Blobs are not retry-able stream, return false
        return false;
    }
}
exports.GenericRetrier = GenericRetrier;
GenericRetrier.OPC_CLIENT_RETRIES_HEADER = "opc-client-retries";
GenericRetrier.OCI_SDK_DEFAULT_RETRY_ENABLED = "OCI_SDK_DEFAULT_RETRY_ENABLED";
GenericRetrier.DefaultRetryConfiguration = GenericRetrier.envVariableCheckForDefaultRetry;
//# sourceMappingURL=retrier.js.map