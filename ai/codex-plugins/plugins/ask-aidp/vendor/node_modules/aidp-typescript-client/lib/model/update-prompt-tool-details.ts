// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a Prompt Tool
*/
export interface UpdatePromptToolDetails extends model.UpdateToolDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.PromptToolConfiguration;

   "toolType": string;
}

export namespace UpdatePromptToolDetails {



    export function getJsonObj(obj: UpdatePromptToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getJsonObj(obj) as UpdatePromptToolDetails, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'PROMPT';
    export function getDeserializedJsonObj(obj: UpdatePromptToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getDeserializedJsonObj(obj) as UpdatePromptToolDetails, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
