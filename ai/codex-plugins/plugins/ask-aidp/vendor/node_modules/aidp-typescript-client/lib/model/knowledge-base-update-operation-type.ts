// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Update supported in KnowledgeBase/source
**/
export enum KnowledgeBaseUpdateOperationType {
    MetadataUpdate = "METADATA_UPDATE",
    SourcesUpdate = "SOURCES_UPDATE",
    AddIndex = "ADD_INDEX",
    DropIndex = "DROP_INDEX"
    
}

export namespace KnowledgeBaseUpdateOperationType {
    export function getJsonObj(obj: KnowledgeBaseUpdateOperationType): KnowledgeBaseUpdateOperationType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseUpdateOperationType): KnowledgeBaseUpdateOperationType {
        return obj;
    }
}

