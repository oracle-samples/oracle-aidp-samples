// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a Prompt Tool
*/
export interface CreatePromptToolDetails extends model.CreateToolDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.PromptToolConfiguration;

   "toolType": string;
}

export namespace CreatePromptToolDetails {



    export function getJsonObj(obj: CreatePromptToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getJsonObj(obj) as CreatePromptToolDetails, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'PROMPT';
    export function getDeserializedJsonObj(obj: CreatePromptToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getDeserializedJsonObj(obj) as CreatePromptToolDetails, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
