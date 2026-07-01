// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Safety guardrails configuration for an agent flow
*/
export interface GuardrailsConfiguration {
    /**
    * The unique identifier (UUID) of the guardrails configuration.
    */
    'key'?: string;
    /**
    * Name of the guardrails configuration
    */
    'name'?: string;
    /**
    * Description of the guardrails configuration
    */
    'description'?: string;
    /**
    * List of safety policies configured in this guardrails
    */
    'policies'?: Array<model.SafetyPolicy>;

}

export namespace GuardrailsConfiguration {





    export function getJsonObj(obj: GuardrailsConfiguration): object {
        const jsonObj = {...obj, ...{
            



                'policies': obj.policies ?
                
                obj.policies.map((item)=>{return model.SafetyPolicy.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GuardrailsConfiguration): object {
        const jsonObj = {...obj, ...{
            



                    'policies': obj.policies ?
                
                obj.policies.map((item)=>{return model.SafetyPolicy.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
