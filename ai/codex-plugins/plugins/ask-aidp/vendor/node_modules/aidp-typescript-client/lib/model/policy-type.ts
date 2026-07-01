// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of safety guardrail policy
**/
export enum PolicyType {
    ContentModeration = "CONTENT_MODERATION",
    PromptAttacksPrevention = "PROMPT_ATTACKS_PREVENTION",
    PiiDetection = "PII_DETECTION",
    DeniedTopics = "DENIED_TOPICS",
    WordFilters = "WORD_FILTERS",
    ContextualGrounding = "CONTEXTUAL_GROUNDING",
    CustomPolicy = "CUSTOM_POLICY"
    
}

export namespace PolicyType {
    export function getJsonObj(obj: PolicyType): PolicyType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: PolicyType): PolicyType {
        return obj;
    }
}

