// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a Knowledge Base which could be an inherited privilege coming from object up in hierarchy.
**/
export enum AllKnowledgeBasePrivilege {
    Write = "WRITE",
    Admin = "ADMIN",
    Select = "SELECT",
    Manage = "MANAGE"
    
}

export namespace AllKnowledgeBasePrivilege {
    export function getJsonObj(obj: AllKnowledgeBasePrivilege): AllKnowledgeBasePrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AllKnowledgeBasePrivilege): AllKnowledgeBasePrivilege {
        return obj;
    }
}

