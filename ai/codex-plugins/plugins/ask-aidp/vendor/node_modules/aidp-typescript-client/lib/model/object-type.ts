// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of Object
**/
export enum ObjectType {
    DataLake = "DATA_LAKE",
    Role = "ROLE",
    Catalog = "CATALOG",
    Schema = "SCHEMA",
    Table = "TABLE",
    Volume = "VOLUME",
    View = "VIEW",
    Workspace = "WORKSPACE",
    WorkspaceFile = "WORKSPACE_FILE",
    WorkspaceFolder = "WORKSPACE_FOLDER",
    Compute = "COMPUTE",
    Folder = "FOLDER",
    File = "FILE",
    AutoPopulate = "AUTO_POPULATE",
    VolumeFile = "VOLUME_FILE",
    VolumeFolder = "VOLUME_FOLDER",
    KnowledgeBase = "KNOWLEDGE_BASE",
    KnowledgeBaseJob = "KNOWLEDGE_BASE_JOB",
    KnowledgeBaseJobRun = "KNOWLEDGE_BASE_JOB_RUN",
    KnowledgeBaseOwnership = "KNOWLEDGE_BASE_OWNERSHIP",
    VectorStoreCredentials = "VECTOR_STORE_CREDENTIALS",
    AuditLog = "AUDIT_LOG",
    Workflow = "WORKFLOW",
    Credential = "CREDENTIAL",
    AgentFlow = "AGENT_FLOW",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ObjectType {
    export function getJsonObj(obj: ObjectType): ObjectType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ObjectType): ObjectType {
        return obj;
    }
}

