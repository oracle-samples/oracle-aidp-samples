/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import "isomorphic-fetch";
import { RequestSigner } from "./signer";
import { HttpRequest } from "./http-request";
declare const Breaker: any;
export interface HttpClient {
    send(req: HttpRequest, forceExcludeBody?: boolean, targetService?: string, operationName?: string, timestamp?: string, endpoint?: string, apiReferenceLink?: string): Promise<Response>;
}
export declare class FetchHttpClient implements HttpClient {
    private readonly signer;
    private circuitBreaker;
    private httpOptions;
    private bodyDuplexMode;
    private static BODY_DUPLEX_KEY;
    private static DEFAULT_DUPLEX_VALUE;
    constructor(signer: RequestSigner | null, circuitBreaker?: typeof Breaker, httpOptions?: {
        [key: string]: any;
    } | undefined, bodyDuplexMode?: any);
    send(req: HttpRequest, forceExcludeBody?: boolean, targetService?: string, operationName?: string, timestamp?: string, endpoint?: string, apiReferenceLink?: string): Promise<Response>;
}
export {};
