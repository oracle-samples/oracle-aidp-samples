// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for Prompt Tool
*/
export interface PromptTool extends model.Tool {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.PromptToolConfiguration;

   "toolType": string;
}

export namespace PromptTool {



    export function getJsonObj(obj: PromptTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getJsonObj(obj) as PromptTool, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'PROMPT';
    export function getDeserializedJsonObj(obj: PromptTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getDeserializedJsonObj(obj) as PromptTool, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.PromptToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
