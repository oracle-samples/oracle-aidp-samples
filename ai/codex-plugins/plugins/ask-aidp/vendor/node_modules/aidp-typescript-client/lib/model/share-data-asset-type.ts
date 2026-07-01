// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The asset type of an operation.
**/
export enum ShareDataAssetType {
    Schema = "SCHEMA",
    Table = "TABLE",
    View = "VIEW",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ShareDataAssetType {
    export function getJsonObj(obj: ShareDataAssetType): ShareDataAssetType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ShareDataAssetType): ShareDataAssetType {
        return obj;
    }
}

