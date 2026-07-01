// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The current lifecycle state of the credential object.
**/
export enum CredentialV2LifeCycleState {
    Active = "ACTIVE",
    Creating = "CREATING",
    Updating = "UPDATING",
    Deleting = "DELETING"
    
}

export namespace CredentialV2LifeCycleState {
    export function getJsonObj(obj: CredentialV2LifeCycleState): CredentialV2LifeCycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CredentialV2LifeCycleState): CredentialV2LifeCycleState {
        return obj;
    }
}

