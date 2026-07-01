// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The role scope based on grantee type.
**/
export enum GetRoleScopeType {
    User = "USER",
    Admin = "ADMIN",
    MemberOf = "MEMBER_OF",
    Group = "GROUP",
    All = "ALL"
    
}

export namespace GetRoleScopeType {
    export function getJsonObj(obj: GetRoleScopeType): GetRoleScopeType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: GetRoleScopeType): GetRoleScopeType {
        return obj;
    }
}

