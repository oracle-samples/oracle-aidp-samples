// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Lifecycle state values exposed via the public credential API.
**/
export enum CredentialLifecycleState {
    Active = "ACTIVE",
    Creating = "CREATING",
    Updating = "UPDATING",
    Deleting = "DELETING",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace CredentialLifecycleState {
    export function getJsonObj(obj: CredentialLifecycleState): CredentialLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CredentialLifecycleState): CredentialLifecycleState {
        return obj;
    }
}

