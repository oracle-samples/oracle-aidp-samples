// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Table type.
**/
export enum TableType {
    Managed = "MANAGED",
    External = "EXTERNAL",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace TableType {
    export function getJsonObj(obj: TableType): TableType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: TableType): TableType {
        return obj;
    }
}

