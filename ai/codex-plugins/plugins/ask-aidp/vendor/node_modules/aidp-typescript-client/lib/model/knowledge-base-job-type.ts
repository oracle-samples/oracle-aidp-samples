// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of KnowledgeBase Job Definition supported
**/
export enum KnowledgeBaseJobType {
    Scheduled = "SCHEDULED",
    OnDemand = "ON_DEMAND"
    
}

export namespace KnowledgeBaseJobType {
    export function getJsonObj(obj: KnowledgeBaseJobType): KnowledgeBaseJobType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobType): KnowledgeBaseJobType {
        return obj;
    }
}

