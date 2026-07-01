// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of Vector Store supported where Embedding is stored
**/
export enum KnowledgeBaseVectorDbType {
    Adw23Ai = "ADW_23_AI",
    Adw26Ai = "ADW_26_AI"
    
}

export namespace KnowledgeBaseVectorDbType {
    export function getJsonObj(obj: KnowledgeBaseVectorDbType): KnowledgeBaseVectorDbType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseVectorDbType): KnowledgeBaseVectorDbType {
        return obj;
    }
}

