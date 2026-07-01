// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of Sources supported where Embedding is generated
**/
export enum KnowledgeBaseEmbeddingModelSourceType {
    Adw23Ai = "ADW_23_AI",
    Adw26Ai = "ADW_26_AI",
    GenAi = "GEN_AI"
    
}

export namespace KnowledgeBaseEmbeddingModelSourceType {
    export function getJsonObj(obj: KnowledgeBaseEmbeddingModelSourceType): KnowledgeBaseEmbeddingModelSourceType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseEmbeddingModelSourceType): KnowledgeBaseEmbeddingModelSourceType {
        return obj;
    }
}

