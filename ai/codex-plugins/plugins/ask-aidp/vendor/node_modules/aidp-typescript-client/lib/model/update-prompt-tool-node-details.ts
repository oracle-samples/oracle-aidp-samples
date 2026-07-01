// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a Prompt Tool
*/
export interface UpdatePromptToolNodeDetails extends model.UpdateAgentFlowNodeDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace UpdatePromptToolNodeDetails {


    export function getJsonObj(obj: UpdatePromptToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getJsonObj(obj) as UpdatePromptToolNodeDetails, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'PROMPT_TOOL';
    export function getDeserializedJsonObj(obj: UpdatePromptToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as UpdatePromptToolNodeDetails, ...{
            

         }};

        
        
        return jsonObj;
    }
}
