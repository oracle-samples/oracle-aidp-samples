// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * List of all privileges in the AI Data Platform Workbench.
**/
export enum AllPrivilegeType {
    User = "USER",
    Administrator = "ADMINISTRATOR",
    Read = "READ",
    Use = "USE",
    Manage = "MANAGE",
    Admin = "ADMIN",
    CreateCatalog = "CREATE_CATALOG",
    Select = "SELECT",
    Insert = "INSERT",
    Update = "UPDATE",
    Delete = "DELETE",
    Alter = "ALTER",
    CreateSchema = "CREATE_SCHEMA",
    Write = "WRITE",
    CreateView = "CREATE_VIEW",
    CreateVolume = "CREATE_VOLUME",
    CreateTable = "CREATE_TABLE",
    CreateKnowledgeBase = "CREATE_KNOWLEDGE_BASE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace AllPrivilegeType {
    export function getJsonObj(obj: AllPrivilegeType): AllPrivilegeType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AllPrivilegeType): AllPrivilegeType {
        return obj;
    }
}

