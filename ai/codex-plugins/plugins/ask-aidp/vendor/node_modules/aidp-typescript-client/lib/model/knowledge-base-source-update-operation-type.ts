// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Update action type supported on sources in KnowledgeBase
**/
export enum KnowledgeBaseSourceUpdateOperationType {
    AddSource = "ADD_SOURCE",
    DeleteSource = "DELETE_SOURCE"
    
}

export namespace KnowledgeBaseSourceUpdateOperationType {
    export function getJsonObj(obj: KnowledgeBaseSourceUpdateOperationType): KnowledgeBaseSourceUpdateOperationType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseSourceUpdateOperationType): KnowledgeBaseSourceUpdateOperationType {
        return obj;
    }
}

