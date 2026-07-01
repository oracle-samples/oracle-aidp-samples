// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The state of the Share
**/
export enum ShareLifecycleState {
    Active = "ACTIVE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ShareLifecycleState {
    export function getJsonObj(obj: ShareLifecycleState): ShareLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ShareLifecycleState): ShareLifecycleState {
        return obj;
    }
}

