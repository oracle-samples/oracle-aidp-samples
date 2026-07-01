// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The resource type of an async operation.
**/
export enum AsyncOperationResourceType {
    Unknown = "UNKNOWN",
    Catalog = "CATALOG",
    Schema = "SCHEMA",
    Table = "TABLE",
    View = "VIEW",
    Volume = "VOLUME",
    VolumeFile = "VOLUME_FILE",
    Workspace = "WORKSPACE",
    WorkspaceObject = "WORKSPACE_OBJECT",
    Cluster = "CLUSTER",
    AiCompute = "AI_COMPUTE",
    KnowledgeBase = "KNOWLEDGE_BASE",
    KnowledgeBaseJob = "KNOWLEDGE_BASE_JOB",
    KnowledgeBaseJobRun = "KNOWLEDGE_BASE_JOB_RUN",
    AgentFlow = "AGENT_FLOW",
    GitOperation = "GIT_OPERATION",
    BundleOperation = "BUNDLE_OPERATION",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace AsyncOperationResourceType {
    export function getJsonObj(obj: AsyncOperationResourceType): AsyncOperationResourceType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AsyncOperationResourceType): AsyncOperationResourceType {
        return obj;
    }
}

