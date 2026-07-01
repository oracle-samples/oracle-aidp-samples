// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * List of sub-resources that are RBAC managed by AI Data Platform Workbench.
**/
export enum AllResourceType {
    Workspace = "WORKSPACE",
    Workflow = "WORKFLOW",
    Folder = "FOLDER",
    File = "FILE",
    Cluster = "CLUSTER",
    MasterCatalog = "MASTER_CATALOG",
    Catalog = "CATALOG",
    Schema = "SCHEMA",
    Table = "TABLE",
    Share = "SHARE",
    Recipient = "RECIPIENT",
    Volume = "VOLUME",
    View = "VIEW",
    KnowledgeBase = "KNOWLEDGE_BASE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace AllResourceType {
    export function getJsonObj(obj: AllResourceType): AllResourceType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AllResourceType): AllResourceType {
        return obj;
    }
}

