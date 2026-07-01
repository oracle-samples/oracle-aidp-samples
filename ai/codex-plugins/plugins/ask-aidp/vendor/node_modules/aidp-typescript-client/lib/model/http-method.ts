// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * HTTP method for the request
**/
export enum HttpMethod {
    Get = "GET",
    Post = "POST",
    Put = "PUT",
    Delete = "DELETE",
    Patch = "PATCH"
    
}

export namespace HttpMethod {
    export function getJsonObj(obj: HttpMethod): HttpMethod {
        return obj;
    }
    export function getDeserializedJsonObj(obj: HttpMethod): HttpMethod {
        return obj;
    }
}

