// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The current state of the table.
**/
export enum TableLifecycleState {
    Active = "ACTIVE",
    Creating = "CREATING",
    Deleting = "DELETING",
    Updating = "UPDATING",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace TableLifecycleState {
    export function getJsonObj(obj: TableLifecycleState): TableLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: TableLifecycleState): TableLifecycleState {
        return obj;
    }
}

