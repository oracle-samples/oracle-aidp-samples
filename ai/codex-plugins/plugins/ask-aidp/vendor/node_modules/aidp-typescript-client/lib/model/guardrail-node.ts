// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A node used to run guardrails on input in the agent flow.
*/
export interface GuardrailNode extends model.AgentFlowNode {
    /**
    * A reference by key value to a guardrails configuration stored in the guardrails map in the flow diagram.
    */
    'guardrailsConfigKey'?: string;
    /**
    * Extra configuration for the guardrails node.
    */
    'extraGuardrailsConfig'?: { [key: string]: string; };

   "type": string;
}

export namespace GuardrailNode {



    export function getJsonObj(obj: GuardrailNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as GuardrailNode, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const type = 'GUARDRAILS';
    export function getDeserializedJsonObj(obj: GuardrailNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as GuardrailNode, ...{
            


         }};

        
        
        return jsonObj;
    }
}
