// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a View which could be an inherited privilege coming from object up in hierarchy.
**/
export enum ViewPrivilege {
    Select = "SELECT",
    Manage = "MANAGE",
    Write = "WRITE",
    Insert = "INSERT",
    Update = "UPDATE",
    Delete = "DELETE",
    Alter = "ALTER",
    Admin = "ADMIN",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ViewPrivilege {
    export function getJsonObj(obj: ViewPrivilege): ViewPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ViewPrivilege): ViewPrivilege {
        return obj;
    }
}

