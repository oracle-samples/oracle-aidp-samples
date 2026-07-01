// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Privilege for a cluster.
**/
export enum ClusterPrivilege {
    Read = "READ",
    Use = "USE",
    Admin = "ADMIN",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ClusterPrivilege {
    export function getJsonObj(obj: ClusterPrivilege): ClusterPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ClusterPrivilege): ClusterPrivilege {
        return obj;
    }
}

