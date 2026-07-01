// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Individual safety policy configuration
*/
export interface SafetyPolicy {
    /**
    * Custom name for the policy
    */
    'policyName'?: string;
    /**
    * Description of the policy
    */
    'policyDescription'?: string;
    /**
    * Scope of policy application
    */
    'scope': model.PolicyScope;
    /**
    * Action to take when policy is violated
    */
    'action': model.PolicyAction;
    /**
    * Threshold value for policy violation (0.0 to 1.0) Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'threshold'?: number;

   "policyType": string;
}

export namespace SafetyPolicy {






    export function getJsonObj(obj: SafetyPolicy): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        if (obj && "policyType" in obj && obj.policyType) {
            switch (obj.policyType) {
                case "PII_DETECTION":
                    return model.PiiDetectionPolicy.getJsonObj(<model.PiiDetectionPolicy>(<object>jsonObj), true);
                case "DENIED_TOPICS":
                    return model.DeniedTopicsPolicy.getJsonObj(<model.DeniedTopicsPolicy>(<object>jsonObj), true);
                case "CONTENT_MODERATION":
                    return model.ContentModerationPolicy.getJsonObj(<model.ContentModerationPolicy>(<object>jsonObj), true);
                case "WORD_FILTERS":
                    return model.WordFiltersPolicy.getJsonObj(<model.WordFiltersPolicy>(<object>jsonObj), true);
                case "PROMPT_ATTACKS_PREVENTION":
                    return model.PromptAttacksPreventionPolicy.getJsonObj(<model.PromptAttacksPreventionPolicy>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.policyType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SafetyPolicy): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        if (obj && "policyType" in obj && obj.policyType) {
            switch (obj.policyType) {
                case "PII_DETECTION":
                    return model.PiiDetectionPolicy.getDeserializedJsonObj(<model.PiiDetectionPolicy>(<object>jsonObj), true);
                case "DENIED_TOPICS":
                    return model.DeniedTopicsPolicy.getDeserializedJsonObj(<model.DeniedTopicsPolicy>(<object>jsonObj), true);
                case "CONTENT_MODERATION":
                    return model.ContentModerationPolicy.getDeserializedJsonObj(<model.ContentModerationPolicy>(<object>jsonObj), true);
                case "WORD_FILTERS":
                    return model.WordFiltersPolicy.getDeserializedJsonObj(<model.WordFiltersPolicy>(<object>jsonObj), true);
                case "PROMPT_ATTACKS_PREVENTION":
                    return model.PromptAttacksPreventionPolicy.getDeserializedJsonObj(<model.PromptAttacksPreventionPolicy>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.policyType}`)
        }
        }
        return jsonObj;
    }
}
