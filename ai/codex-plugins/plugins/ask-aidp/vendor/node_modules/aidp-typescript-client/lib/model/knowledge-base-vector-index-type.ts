// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of Vector Index
**/
export enum KnowledgeBaseVectorIndexType {
    Hnsw = "HNSW",
    Ivf = "IVF"
    
}

export namespace KnowledgeBaseVectorIndexType {
    export function getJsonObj(obj: KnowledgeBaseVectorIndexType): KnowledgeBaseVectorIndexType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseVectorIndexType): KnowledgeBaseVectorIndexType {
        return obj;
    }
}

