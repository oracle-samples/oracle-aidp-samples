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
exports.composeRequest = void 0;
const helper_1 = require("./helper");
const retry_token_header_1 = require("./retry-token-header");
/*
 * Composes and Signs an SDKRequest
 * @param RequestParams to create a request
 */
function composeRequest(params) {
    return __awaiter(this, void 0, void 0, function* () {
        const headers = computeHeaders(params);
        const uri = computeUri(params);
        let body = params.bodyContent;
        // If body exists, check if content-length exists and check if user wants to back up binary request body
        if (body) {
            const content = yield helper_1.autoDetectContentLengthAndReadBody(headers, params);
            body = content ? content : body;
        }
        if (body === "{}") {
            return {
                method: params.method,
                headers: headers,
                uri: uri
            };
        }
        else {
            return {
                method: params.method,
                headers: headers,
                uri: uri,
                body: body
            };
        }
    });
}
exports.composeRequest = composeRequest;
function computeUri(params) {
    const path = validateAndComputePath(params.path, params.pathParams);
    const queryString = stringify(params.queryParams);
    let uri = params.baseEndpoint + path;
    if (queryString) {
        uri = uri + "?" + queryString;
    }
    return uri;
}
function validateAndComputePath(path, pathParams) {
    if (pathParams) {
        for (const [key, value] of Object.entries(pathParams)) {
            if (value || value === false || value === 0) {
                path = path.replace(key, encodeURIComponent(String(value)));
            }
            else {
                throw new Error(`Missing required path parameter for ${key}`);
            }
        }
    }
    return path;
}
function computeHeaders(params) {
    const headers = new Headers();
    for (const [key, value] of Object.entries(params.defaultHeaders)) {
        headers.append(key, String(value));
    }
    if (params.headerParams) {
        for (const [key, value] of Object.entries(params.headerParams)) {
            if (value) {
                if (key == "range") {
                    const { startByte, endByte } = value;
                    const rangeValue = `bytes=${startByte}-${endByte}`;
                    headers.append(key, rangeValue);
                }
                else {
                    headers.append(key, String(value));
                }
            }
        }
    }
    if (params.headerParams && retry_token_header_1.OPC_RETRY_TOKEN_HEADER in params.headerParams) {
        retry_token_header_1.addRetryToken(headers);
    }
    helper_1.addAdditionalHeaders(headers, params);
    return headers;
}
function stringify(queryParams) {
    let qs = "";
    if (queryParams) {
        qs = Object.keys(queryParams)
            .map(function (key) {
            let value = queryParams[key];
            if (Array.isArray(value)) {
                let formatter = encoderforArrayFormat();
                return value.reduce(formatter(key), []).join("&");
            }
            // Format Date Object to RFC3339 timestamp string.
            if (Object.prototype.toString.call(value) === "[object Date]" && value instanceof Date) {
                return key + "=" + helper_1.formatDateToRFC3339(value);
            }
            else if (value || value === false || value === 0) {
                return key + "=" + value;
            }
        })
            .filter(key => key !== undefined && key !== null)
            .join("&");
    }
    return qs;
}
function encoderforArrayFormat() {
    return (key) => (result, value) => {
        if (value === undefined || value === null || value === "") {
            return result;
        }
        return [...result, [key, "=", value].join("")];
    };
}
//# sourceMappingURL=request-generator.js.map