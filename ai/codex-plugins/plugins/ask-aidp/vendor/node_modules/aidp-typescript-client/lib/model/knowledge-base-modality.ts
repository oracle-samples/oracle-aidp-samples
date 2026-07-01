// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of modalities supported in KnowledgeBase
**/
export enum KnowledgeBaseModality {
    Text = "TEXT"
    
}

export namespace KnowledgeBaseModality {
    export function getJsonObj(obj: KnowledgeBaseModality): KnowledgeBaseModality {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseModality): KnowledgeBaseModality {
        return obj;
    }
}

