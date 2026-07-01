// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of KB Job Run Trigger  supported
**/
export enum KnowledgeBaseJobRunTriggerType {
    Notebook = "NOTEBOOK",
    Other = "OTHER"
    
}

export namespace KnowledgeBaseJobRunTriggerType {
    export function getJsonObj(obj: KnowledgeBaseJobRunTriggerType): KnowledgeBaseJobRunTriggerType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobRunTriggerType): KnowledgeBaseJobRunTriggerType {
        return obj;
    }
}

