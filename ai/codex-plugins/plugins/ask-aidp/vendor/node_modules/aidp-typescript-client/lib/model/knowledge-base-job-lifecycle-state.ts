// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Lifecycle state for KB JOB
**/
export enum KnowledgeBaseJobLifecycleState {
    Active = "ACTIVE",
    Inactive = "INACTIVE",
    Creating = "CREATING",
    Deleting = "DELETING"
    
}

export namespace KnowledgeBaseJobLifecycleState {
    export function getJsonObj(obj: KnowledgeBaseJobLifecycleState): KnowledgeBaseJobLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobLifecycleState): KnowledgeBaseJobLifecycleState {
        return obj;
    }
}

