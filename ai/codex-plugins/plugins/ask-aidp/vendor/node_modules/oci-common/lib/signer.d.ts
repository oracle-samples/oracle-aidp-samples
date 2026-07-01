/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import auth = require("./auth/auth");
import { Method } from "./request-generator";
import { HttpRequest } from "./http-request";
export declare class SignerRequest {
    private headers;
    method: string;
    path?: string | null;
    constructor(method: Method, url: string, headers: Headers);
    getHeader(name: string): string | null;
    setHeader(name: string, value: any): void;
}
/**
 * An interface signs the http request.
 */
export interface RequestSigner {
    /**
     * Sign the http request.
     * @param request http request .
     * @param forceExcludeBody exclude body or not.
     */
    signHttpRequest(request: HttpRequest, forceExcludeBody?: boolean): void;
}
/**
 * The default implementation of [[RequestSigner]].
 */
export declare class DefaultRequestSigner implements RequestSigner {
    private authenticationDetailsProvider;
    private static readonly headersToSign;
    private static readonly methodsThatRequireExtraHeaders;
    private delegationToken;
    private privateKeyBuffer;
    private privateKey;
    /**
     * Construct an instance of [[DefaultRequestSigner]].
     * @param authenticationDetailsProvider the authentication details provider.
     */
    constructor(authenticationDetailsProvider: auth.AuthenticationDetailsProvider);
    /**
     * Sign the http request.
     * @param request http request.
     * @param forceExcludeBody exclude body or not.
     */
    signHttpRequest(request: HttpRequest, forceExcludeBody?: boolean): Promise<void>;
}
