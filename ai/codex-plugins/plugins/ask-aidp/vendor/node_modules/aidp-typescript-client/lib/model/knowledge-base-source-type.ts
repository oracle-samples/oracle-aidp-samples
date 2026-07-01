// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of KnowledgeBase Sources
**/
export enum KnowledgeBaseSourceType {
    Volume = "VOLUME",
    Table = "TABLE"
    
}

export namespace KnowledgeBaseSourceType {
    export function getJsonObj(obj: KnowledgeBaseSourceType): KnowledgeBaseSourceType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseSourceType): KnowledgeBaseSourceType {
        return obj;
    }
}

