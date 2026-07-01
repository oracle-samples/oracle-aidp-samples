// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a volume. Can be an inherited privilege coming from object up in hierarchy.
**/
export enum AllVolumePrivilege {
    Read = "READ",
    Write = "WRITE",
    Admin = "ADMIN",
    Select = "SELECT",
    Manage = "MANAGE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace AllVolumePrivilege {
    export function getJsonObj(obj: AllVolumePrivilege): AllVolumePrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AllVolumePrivilege): AllVolumePrivilege {
        return obj;
    }
}

