// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for Custom Tool
*/
export interface CustomTool extends model.Tool {
    /**
    * The provider of the tool, default is AIDP
    */
    'toolProvider'?: string;
    /**
    * The type name for this tool
    */
    'toolTypeName'?: string;
    /**
    * The list of named properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.CustomToolConfiguration;

   "toolType": string;
}

export namespace CustomTool {





    export function getJsonObj(obj: CustomTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getJsonObj(obj) as CustomTool, ...{
            



                'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'CUSTOM';
    export function getDeserializedJsonObj(obj: CustomTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getDeserializedJsonObj(obj) as CustomTool, ...{
            



                    'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
