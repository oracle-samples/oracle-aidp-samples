"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
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
exports.FetchHttpClient = void 0;
const promise = __importStar(require("es6-promise"));
require("isomorphic-fetch");
const helper_1 = require("./helper");
const Breaker = require("opossum");
promise.polyfill();
class FetchHttpClient {
    constructor(signer, circuitBreaker, httpOptions, bodyDuplexMode) {
        this.signer = signer;
        this.circuitBreaker = null;
        this.httpOptions = undefined;
        if (circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }
        if (httpOptions)
            this.httpOptions = httpOptions;
        if (bodyDuplexMode)
            this.bodyDuplexMode = bodyDuplexMode;
    }
    send(req, forceExcludeBody = false, targetService = "", operationName = "", timestamp = new Date().toISOString(), endpoint = "", apiReferenceLink = "") {
        return __awaiter(this, void 0, void 0, function* () {
            // Get Request body
            const body = helper_1.getSignerAndReqBody(req.body, forceExcludeBody);
            // Sign Request
            if (this.signer) {
                yield this.signer.signHttpRequest({
                    method: req.method,
                    headers: req.headers,
                    uri: req.uri,
                    body: body.signerBody
                }, forceExcludeBody);
            }
            const reqInit = {
                method: req.method,
                headers: req.headers,
                body: body.requestBody
            };
            if (body.requestBody) {
                reqInit[FetchHttpClient.BODY_DUPLEX_KEY] = this.bodyDuplexMode
                    ? this.bodyDuplexMode
                    : FetchHttpClient.DEFAULT_DUPLEX_VALUE;
            }
            const request = new Request(req.uri, reqInit);
            // Send Request
            // Need to convert to type RequestInit for Fetch() type compatibility
            let options = this.httpOptions;
            if (this.circuitBreaker) {
                return options
                    ? this.circuitBreaker.fire(request, options, targetService, operationName, timestamp, endpoint, apiReferenceLink)
                    : this.circuitBreaker
                        .fire(request, undefined, targetService, operationName, timestamp, endpoint, apiReferenceLink)
                        .then((e) => {
                        return e.response ? e.response : e;
                    })
                        .catch((e) => {
                        if (e.response) {
                            // If error contains response field, it is an actual server error, return it.
                            return e.response;
                        }
                        else {
                            // These are client side error. Throw exception.
                            throw e;
                        }
                    });
            }
            else {
                return options ? fetch(request, options) : fetch(request);
            }
        });
    }
}
exports.FetchHttpClient = FetchHttpClient;
FetchHttpClient.BODY_DUPLEX_KEY = "duplex";
FetchHttpClient.DEFAULT_DUPLEX_VALUE = "half";
//# sourceMappingURL=http.js.map