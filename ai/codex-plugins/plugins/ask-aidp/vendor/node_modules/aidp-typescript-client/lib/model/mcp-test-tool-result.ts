// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response when toolType = MCP.
*/
export interface McpTestToolResult extends model.TestToolResult {
    'result': model.McpResult;

   "toolType": string;
}

export namespace McpTestToolResult {


    export function getJsonObj(obj: McpTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getJsonObj(obj) as McpTestToolResult, ...{
            
                'result': obj.result ?
                
                
                model.McpResult.getJsonObj(obj.result) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'MCP';
    export function getDeserializedJsonObj(obj: McpTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getDeserializedJsonObj(obj) as McpTestToolResult, ...{
            
                    'result': obj.result ?
                
                
                model.McpResult.getDeserializedJsonObj(obj.result) : undefined,
         }};

        
        
        return jsonObj;
    }
}
