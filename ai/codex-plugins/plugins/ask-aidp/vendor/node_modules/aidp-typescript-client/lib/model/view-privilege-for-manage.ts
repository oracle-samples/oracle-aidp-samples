// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a view.
**/
export enum ViewPrivilegeForManage {
    Select = "SELECT",
    Alter = "ALTER",
    Admin = "ADMIN"
    
}

export namespace ViewPrivilegeForManage {
    export function getJsonObj(obj: ViewPrivilegeForManage): ViewPrivilegeForManage {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ViewPrivilegeForManage): ViewPrivilegeForManage {
        return obj;
    }
}

