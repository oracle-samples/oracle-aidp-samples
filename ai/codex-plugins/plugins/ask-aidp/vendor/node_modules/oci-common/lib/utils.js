"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

 * Utility method to check if environment is node or browser
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.isCircuitBreakerSystemEnabled = exports.checkNotNull = exports.isEmpty = exports.isBrowser = void 0;
const circuit_breaker_1 = __importDefault(require("./circuit-breaker"));
function isBrowser() {
    if (typeof window === "undefined") {
        return false;
    }
    return true;
}
exports.isBrowser = isBrowser;
// utility method checks if object is empty or not
function isEmpty(obj) {
    return Object.keys(obj).length === 0;
}
exports.isEmpty = isEmpty;
// Utility method to check if value is null, else throw error with msg.
function checkNotNull(value, msg) {
    if (value) {
        return value;
    }
    throw new Error(msg);
}
exports.checkNotNull = checkNotNull;
// Utility method to check if circuit breaker should be created
// This is used when no circuit breaker was created for the client, if not: Check if it was disabled at the client level / global level / environment level in respective order
// return false means do not create circuit breaker.
function isCircuitBreakerSystemEnabled(clientConfiguration) {
    // Client level check
    const clientLevelCheck = clientConfiguration &&
        clientConfiguration.circuitBreaker &&
        clientConfiguration.circuitBreaker.noCircuit;
    if (clientLevelCheck ||
        !circuit_breaker_1.default.EnableGlobalCircuitBreaker ||
        circuit_breaker_1.default.EnableDefaultCircuitBreaker === "false") {
        return false;
    }
    return true;
}
exports.isCircuitBreakerSystemEnabled = isCircuitBreakerSystemEnabled;
//# sourceMappingURL=utils.js.map