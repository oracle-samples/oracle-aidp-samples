/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import { Range } from "./range";
import { HttpRequest } from "./http-request";
export interface Params {
    [key: string]: string | Date | Range | string[] | number | number[] | boolean | boolean[] | undefined;
}
export declare type Method = "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS" | "TRACE" | "CONNECT";
export interface RequestParams {
    readonly baseEndpoint: string;
    readonly path: string;
    readonly defaultHeaders: Params;
    readonly method: Method;
    readonly bodyContent?: any;
    readonly pathParams?: Params;
    readonly headerParams?: Params;
    readonly queryParams?: Params;
    readonly formParam?: Params;
    readonly backupBinaryBody?: boolean;
}
export declare function composeRequest(params: RequestParams): Promise<HttpRequest>;
