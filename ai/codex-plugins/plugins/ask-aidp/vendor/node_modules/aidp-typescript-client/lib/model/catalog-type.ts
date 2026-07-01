// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The catalog type. Internal (Standard) or external.
**/
export enum CatalogType {
    External = "EXTERNAL",
    Internal = "INTERNAL",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace CatalogType {
    export function getJsonObj(obj: CatalogType): CatalogType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CatalogType): CatalogType {
        return obj;
    }
}

