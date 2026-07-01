// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The current state of the schema.
**/
export enum SchemaLifecycleState {
    Active = "ACTIVE",
    Creating = "CREATING",
    Deleting = "DELETING",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace SchemaLifecycleState {
    export function getJsonObj(obj: SchemaLifecycleState): SchemaLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SchemaLifecycleState): SchemaLifecycleState {
        return obj;
    }
}

