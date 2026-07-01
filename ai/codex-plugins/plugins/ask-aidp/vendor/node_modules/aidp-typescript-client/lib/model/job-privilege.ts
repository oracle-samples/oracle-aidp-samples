// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a job.
**/
export enum JobPrivilege {
    Read = "READ",
    Use = "USE",
    Manage = "MANAGE",
    Admin = "ADMIN",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace JobPrivilege {
    export function getJsonObj(obj: JobPrivilege): JobPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: JobPrivilege): JobPrivilege {
        return obj;
    }
}

