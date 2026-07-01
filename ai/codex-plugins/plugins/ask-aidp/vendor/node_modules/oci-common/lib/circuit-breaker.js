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
const helper_1 = require("./helper");
const retrier_1 = require("./retrier");
const log_1 = require("./log");
const Breaker = require("opossum");
function FetchWrapper(req, options, targetService, operationName, timestamp, endpoint, apiReferenceLink) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                const response = yield fetch(req, options);
                if (response.status && response.status >= 200 && response.status <= 299) {
                    resolve({ response });
                }
                else {
                    const responseClone = response.clone();
                    const errBody = yield helper_1.handleErrorBody(responseClone);
                    const errorObject = helper_1.handleErrorResponse(responseClone, errBody, targetService, operationName, timestamp, endpoint, apiReferenceLink);
                    reject({
                        response,
                        errorObject
                    });
                }
            }
            catch (e) {
                // If we get here, that means response was a client side error
                reject(e);
            }
        }));
    });
}
function defaultErrorFilterFunction(e) {
    if (log_1.LOG.logger)
        log_1.LOG.logger.error("error from defaultErrorFunction: ", e);
    // Only consider client side errors or retry-able server errors
    if (e.code || (e.errorObject && retrier_1.DefaultRetryCondition.shouldBeRetried(e.errorObject))) {
        return false;
    }
    return true;
}
class CircuitBreaker {
    constructor(options) {
        this.circuit = null;
        this.noCircuit = false;
        if (options === null || options === void 0 ? void 0 : options.disableClientCircuitBreaker) {
            this.noCircuit = true;
            return;
        }
        this.circuit = options
            ? new Breaker(FetchWrapper, options)
            : new Breaker(FetchWrapper, CircuitBreaker.DefaultConfiguration);
        // Add emitters
        this.circuit.on("open", () => {
            if (log_1.LOG.logger)
                log_1.LOG.logger.debug("circuit breaker is now in OPEN state");
        });
        this.circuit.on("halfOpen", () => {
            if (log_1.LOG.logger)
                log_1.LOG.logger.debug("circuit breaker is now in HALF OPEN state");
        });
        this.circuit.on("close", () => {
            if (log_1.LOG.logger)
                log_1.LOG.logger.debug("circuit breaker is now in CLOSE state");
        });
        this.circuit.on("shutdown", () => {
            if (log_1.LOG.logger)
                log_1.LOG.logger.debug("circuit breaker is now SHUTDOWN");
        });
    }
    static get envVariableCheckForDefaultCircuitBreaker() {
        if (process.env.OCI_SDK_DEFAULT_CIRCUITBREAKER_ENABLED === "true") {
            CircuitBreaker.DefaultCircuitBreakerOverriden = true;
        }
        else if (process.env.OCI_SDK_DEFAULT_CIRCUITBREAKER_ENABLED === "false") {
            CircuitBreaker.DefaultCircuitBreakerOverriden = true;
        }
        return process.env.OCI_SDK_DEFAULT_CIRCUITBREAKER_ENABLED;
    }
    static get defaultAuthConfiguration() {
        return CircuitBreaker.DefaultAuthConfiguration;
    }
    static get defaultConfiguration() {
        return CircuitBreaker.DefaultConfiguration;
    }
    static set defaultConfiguration(circuitBreakerConfig) {
        CircuitBreaker.DefaultConfiguration = Object.assign(Object.assign({}, CircuitBreaker.DefaultConfiguration), circuitBreakerConfig);
        CircuitBreaker.DefaultCircuitBreakerOverriden = true;
    }
}
exports.default = CircuitBreaker;
CircuitBreaker.EnableGlobalCircuitBreaker = true; // Configuration to turn on/off the global circuit breaker.
CircuitBreaker.EnableDefaultCircuitBreaker = CircuitBreaker.envVariableCheckForDefaultCircuitBreaker;
CircuitBreaker.DefaultCircuitBreakerOverriden = false;
CircuitBreaker.DefaultConfiguration = {
    timeout: 3600000,
    errorThresholdPercentage: 80,
    resetTimeout: 30000,
    rollingCountTimeout: 120000,
    rollingCountBuckets: 120,
    volumeThreshold: 10,
    errorFilter: defaultErrorFilterFunction
};
CircuitBreaker.MIN_WAIT_DURATION_IN_OPEN_STATE_FOR_AUTH_CLIENT_CB = 30;
CircuitBreaker.MAX_WAIT_DURATION_IN_OPEN_STATE_FOR_AUTH_CLIENT_CB = 49;
CircuitBreaker.DefaultAuthConfiguration = {
    timeout: 60000,
    errorThresholdPercentage: 65,
    resetTimeout: Math.floor(Math.random() *
        (CircuitBreaker.MAX_WAIT_DURATION_IN_OPEN_STATE_FOR_AUTH_CLIENT_CB -
            CircuitBreaker.MIN_WAIT_DURATION_IN_OPEN_STATE_FOR_AUTH_CLIENT_CB +
            1) +
        CircuitBreaker.MIN_WAIT_DURATION_IN_OPEN_STATE_FOR_AUTH_CLIENT_CB) * 1000,
    rollingCountTimeout: 120000,
    rollingCountBuckets: 120,
    volumeThreshold: 3,
    errorFilter: (e) => {
        return false;
    } // Treat all exceptions as failure
};
//# sourceMappingURL=circuit-breaker.js.map