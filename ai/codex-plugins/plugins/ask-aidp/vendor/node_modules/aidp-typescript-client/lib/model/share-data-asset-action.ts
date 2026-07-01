// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The action of an operation.
**/
export enum ShareDataAssetAction {
    Add = "ADD",
    Remove = "REMOVE"
    
}

export namespace ShareDataAssetAction {
    export function getJsonObj(obj: ShareDataAssetAction): ShareDataAssetAction {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ShareDataAssetAction): ShareDataAssetAction {
        return obj;
    }
}

