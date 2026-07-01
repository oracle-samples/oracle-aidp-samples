// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Status of log
**/
export enum Status {
    Succeeded = "SUCCEEDED",
    Failed = "FAILED",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace Status {
    export function getJsonObj(obj: Status): Status {
        return obj;
    }
    export function getDeserializedJsonObj(obj: Status): Status {
        return obj;
    }
}

