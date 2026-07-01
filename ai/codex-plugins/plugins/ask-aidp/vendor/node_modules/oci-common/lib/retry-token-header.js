"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

 * This header is excluded from headers.ts as we need to use same retry token but new request token
 * which needs to be incorporated when we add retry strategy.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.addRetryToken = exports.OPC_RETRY_TOKEN_HEADER = void 0;
const uuidv1 = require("uuid/v1");
exports.OPC_RETRY_TOKEN_HEADER = "opc-retry-token";
function addRetryToken(headers) {
    // if user has not passed opc-retry-token
    if (!hasRetryToken(headers)) {
        headers.append(exports.OPC_RETRY_TOKEN_HEADER, generateRetryToken());
    }
}
exports.addRetryToken = addRetryToken;
function hasRetryToken(headers) {
    if (headers.has(exports.OPC_RETRY_TOKEN_HEADER)) {
        return true;
    }
    return false;
}
function generateRetryToken() {
    return uuidv1();
}
//# sourceMappingURL=retry-token-header.js.map