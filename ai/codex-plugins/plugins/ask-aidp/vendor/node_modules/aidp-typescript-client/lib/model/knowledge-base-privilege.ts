// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a Volume
**/
export enum KnowledgeBasePrivilege {
    Select = "SELECT",
    Manage = "MANAGE",
    Admin = "ADMIN"
    
}

export namespace KnowledgeBasePrivilege {
    export function getJsonObj(obj: KnowledgeBasePrivilege): KnowledgeBasePrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBasePrivilege): KnowledgeBasePrivilege {
        return obj;
    }
}

