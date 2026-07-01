// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a tool.
**/
export enum ToolPrivilegeForManage {
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"
    
}

export namespace ToolPrivilegeForManage {
    export function getJsonObj(obj: ToolPrivilegeForManage): ToolPrivilegeForManage {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ToolPrivilegeForManage): ToolPrivilegeForManage {
        return obj;
    }
}

