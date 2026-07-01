// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Catalog privilege
**/
export enum CatalogPrivilege {
    Select = "SELECT",
    Manage = "MANAGE",
    CreateSchema = "CREATE_SCHEMA",
    Admin = "ADMIN",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace CatalogPrivilege {
    export function getJsonObj(obj: CatalogPrivilege): CatalogPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CatalogPrivilege): CatalogPrivilege {
        return obj;
    }
}

