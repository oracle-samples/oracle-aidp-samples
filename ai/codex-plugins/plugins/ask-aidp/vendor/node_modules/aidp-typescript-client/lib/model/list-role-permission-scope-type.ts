// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The permission scope for a role based on how it was obtained.
**/
export enum ListRolePermissionScopeType {
    Direct = "DIRECT",
    Inherited = "INHERITED",
    All = "ALL"
    
}

export namespace ListRolePermissionScopeType {
    export function getJsonObj(obj: ListRolePermissionScopeType): ListRolePermissionScopeType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ListRolePermissionScopeType): ListRolePermissionScopeType {
        return obj;
    }
}

