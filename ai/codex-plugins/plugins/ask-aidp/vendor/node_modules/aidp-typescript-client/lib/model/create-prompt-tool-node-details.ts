// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a Prompt Tool Node in an Agent Flow
*/
export interface CreatePromptToolNodeDetails extends model.CreateAgentFlowNodeDetails {
    /**
    * The unique identifier (key) of the saved AI tool
    */
    'toolKey'?: string;
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.PromptToolConfiguration;

   "type": string;
}

export namespace CreatePromptToolNodeDetails {




    export function getJsonObj(obj: CreatePromptToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getJsonObj(obj) as CreatePromptToolNodeDetails, ...{
            


                'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'PROMPT_TOOL';
    export function getDeserializedJsonObj(obj: CreatePromptToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as CreatePromptToolNodeDetails, ...{
            


                    'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
