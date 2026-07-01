// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a tool. Can be inherited privilege coming from object higher up in hierarchy.
**/
export enum ToolPrivilege {
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"
    
}

export namespace ToolPrivilege {
    export function getJsonObj(obj: ToolPrivilege): ToolPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ToolPrivilege): ToolPrivilege {
        return obj;
    }
}

