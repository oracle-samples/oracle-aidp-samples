// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of KB Job Goal supported
**/
export enum KnowledgeBaseJobGoalType {
    AddRefreshSource = "ADD_REFRESH_SOURCE",
    DeleteSource = "DELETE_SOURCE"
    
}

export namespace KnowledgeBaseJobGoalType {
    export function getJsonObj(obj: KnowledgeBaseJobGoalType): KnowledgeBaseJobGoalType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobGoalType): KnowledgeBaseJobGoalType {
        return obj;
    }
}

