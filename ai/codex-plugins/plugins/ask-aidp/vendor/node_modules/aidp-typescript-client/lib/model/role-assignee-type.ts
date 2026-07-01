// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The entity type to which a role can be assigned to/revoked from. It is also used for role Admin type.
**/
export enum RoleAssigneeType {
    User = "USER",
    Role = "ROLE",
    Group = "GROUP",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace RoleAssigneeType {
    export function getJsonObj(obj: RoleAssigneeType): RoleAssigneeType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: RoleAssigneeType): RoleAssigneeType {
        return obj;
    }
}

