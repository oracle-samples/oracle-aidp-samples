/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import { ObjectSerializer } from "./object-serializer";
interface ResponseParams<T> {
    readonly responseObject: T;
    readonly responseHeaders: HeaderInfo[];
    readonly body?: ObjectSerializer.BodyType;
    readonly bodyModel?: any;
    readonly bodyKey?: string;
    readonly type?: string;
}
interface HeaderInfo {
    key: string;
    value: string | null;
    dataType: string;
}
export declare function composeResponse<T>(params: ResponseParams<T>): T;
export {};
