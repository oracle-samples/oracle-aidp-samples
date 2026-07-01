// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for Mcp Tool
*/
export interface McpTool extends model.Tool {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig': model.McpToolConfiguration;

   "toolType": string;
}

export namespace McpTool {



    export function getJsonObj(obj: McpTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getJsonObj(obj) as McpTool, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.McpToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'MCP';
    export function getDeserializedJsonObj(obj: McpTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getDeserializedJsonObj(obj) as McpTool, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.McpToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
