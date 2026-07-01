// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a schema.
**/
export enum SchemaPrivilegeForManage {
    Select = "SELECT",
    Write = "WRITE",
    CreateView = "CREATE_VIEW",
    CreateVolume = "CREATE_VOLUME",
    CreateTable = "CREATE_TABLE",
    Admin = "ADMIN",
    CreateKnowledgeBase = "CREATE_KNOWLEDGE_BASE"
    
}

export namespace SchemaPrivilegeForManage {
    export function getJsonObj(obj: SchemaPrivilegeForManage): SchemaPrivilegeForManage {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SchemaPrivilegeForManage): SchemaPrivilegeForManage {
        return obj;
    }
}

