// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The operation that can be performed on this resource.
**/
export enum ParAccessType {
    ObjectRead = "OBJECT_READ",
    ObjectWrite = "OBJECT_WRITE",
    ObjectReadWrite = "OBJECT_READ_WRITE",
    AnyObjectWrite = "ANY_OBJECT_WRITE",
    AnyObjectRead = "ANY_OBJECT_READ",
    AnyObjectReadWrite = "ANY_OBJECT_READ_WRITE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ParAccessType {
    export function getJsonObj(obj: ParAccessType): ParAccessType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ParAccessType): ParAccessType {
        return obj;
    }
}

