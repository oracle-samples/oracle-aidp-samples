// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing Guardrails available for configuration
*/
export interface AgentFlowGuardrailsCollection {
    /**
    * List of safety policies available to configure in this guardrails
    */
    'items': Array<model.AgentFlowGuardrailsSummary>;

}

export namespace AgentFlowGuardrailsCollection {


    export function getJsonObj(obj: AgentFlowGuardrailsCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.AgentFlowGuardrailsSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowGuardrailsCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.AgentFlowGuardrailsSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
