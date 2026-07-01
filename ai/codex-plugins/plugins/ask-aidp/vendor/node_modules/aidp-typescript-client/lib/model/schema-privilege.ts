// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a schema. This privilege could be inherited from the object higher up in hierarchy.
**/
export enum SchemaPrivilege {
    Select = "SELECT",
    Manage = "MANAGE",
    Write = "WRITE",
    CreateView = "CREATE_VIEW",
    CreateVolume = "CREATE_VOLUME",
    CreateTable = "CREATE_TABLE",
    CreateKnowledgeBase = "CREATE_KNOWLEDGE_BASE",
    Admin = "ADMIN",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace SchemaPrivilege {
    export function getJsonObj(obj: SchemaPrivilege): SchemaPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SchemaPrivilege): SchemaPrivilege {
        return obj;
    }
}

