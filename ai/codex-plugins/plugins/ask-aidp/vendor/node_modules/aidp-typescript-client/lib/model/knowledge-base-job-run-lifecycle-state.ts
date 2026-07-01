// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Lifecycle state for KnowledgeBase Job Run
**/
export enum KnowledgeBaseJobRunLifecycleState {
    Accepted = "ACCEPTED",
    Canceling = "CANCELING",
    Canceled = "CANCELED",
    Failed = "FAILED",
    Succeeded = "SUCCEEDED",
    InProgress = "IN_PROGRESS"
    
}

export namespace KnowledgeBaseJobRunLifecycleState {
    export function getJsonObj(obj: KnowledgeBaseJobRunLifecycleState): KnowledgeBaseJobRunLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobRunLifecycleState): KnowledgeBaseJobRunLifecycleState {
        return obj;
    }
}

