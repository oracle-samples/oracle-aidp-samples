// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Lifecycle state for KnowledgeBase
**/
export enum KnowledgeBaseLifecycleState {
    Creating = "CREATING",
    Active = "ACTIVE",
    Updating = "UPDATING",
    Deleting = "DELETING",
    NeedsAttention = "NEEDS_ATTENTION",
    Deleted = "DELETED",
    Failed = "FAILED"
    
}

export namespace KnowledgeBaseLifecycleState {
    export function getJsonObj(obj: KnowledgeBaseLifecycleState): KnowledgeBaseLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseLifecycleState): KnowledgeBaseLifecycleState {
        return obj;
    }
}

