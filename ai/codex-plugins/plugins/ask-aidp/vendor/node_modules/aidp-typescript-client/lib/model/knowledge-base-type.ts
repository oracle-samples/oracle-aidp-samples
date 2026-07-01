// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of KnowledgeBase supported
**/
export enum KnowledgeBaseType {
    Native = "NATIVE"
    
}

export namespace KnowledgeBaseType {
    export function getJsonObj(obj: KnowledgeBaseType): KnowledgeBaseType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseType): KnowledgeBaseType {
        return obj;
    }
}

