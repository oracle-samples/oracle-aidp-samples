// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a table.
**/
export enum TablePrivilegeForManage {
    Select = "SELECT",
    Insert = "INSERT",
    Update = "UPDATE",
    Delete = "DELETE",
    Alter = "ALTER",
    Admin = "ADMIN"
    
}

export namespace TablePrivilegeForManage {
    export function getJsonObj(obj: TablePrivilegeForManage): TablePrivilegeForManage {
        return obj;
    }
    export function getDeserializedJsonObj(obj: TablePrivilegeForManage): TablePrivilegeForManage {
        return obj;
    }
}

