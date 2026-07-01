// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The role type for a role.
**/
export enum RoleType {
    System = "SYSTEM",
    Custom = "CUSTOM",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace RoleType {
    export function getJsonObj(obj: RoleType): RoleType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: RoleType): RoleType {
        return obj;
    }
}

