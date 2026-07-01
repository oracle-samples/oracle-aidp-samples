// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Tool Node in an Agent Flow
*/
export interface PromptToolNode extends model.AgentFlowNode {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace PromptToolNode {


    export function getJsonObj(obj: PromptToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as PromptToolNode, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'PROMPT_TOOL';
    export function getDeserializedJsonObj(obj: PromptToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as PromptToolNode, ...{
            

         }};

        
        
        return jsonObj;
    }
}
