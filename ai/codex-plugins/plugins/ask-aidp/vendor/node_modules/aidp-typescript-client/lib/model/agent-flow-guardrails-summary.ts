// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Guardrails Summary information
*/
export interface AgentFlowGuardrailsSummary {
    /**
    * Type of safety policy
    */
    'policyType'?: model.PolicyType;
    /**
    * Custom name for the policy
    */
    'policyName'?: string;
    /**
    * Description of the policy
    */
    'policyDescription'?: string;
    /**
    * Action to take when policy is violated
    */
    'action'?: model.PolicyAction;
    /**
    * Scope of policy application
    */
    'scope'?: model.PolicyScope;
    /**
    * Threshold value for policy violation (0.0 to 1.0) Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'threshold'?: number;
    /**
    * Guardrail categories for this policy and their configurations
    */
    'categories'?: Array<model.CategoryConfig>;

}

export namespace AgentFlowGuardrailsSummary {








    export function getJsonObj(obj: AgentFlowGuardrailsSummary): object {
        const jsonObj = {...obj, ...{
            






                'categories': obj.categories ?
                
                obj.categories.map((item)=>{return model.CategoryConfig.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowGuardrailsSummary): object {
        const jsonObj = {...obj, ...{
            






                    'categories': obj.categories ?
                
                obj.categories.map((item)=>{return model.CategoryConfig.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
