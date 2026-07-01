// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request details when toolType = MCP
*/
export interface TestMcpToolDetails extends model.TestToolDetails {
    'config': model.McpToolConfiguration;
    'mcpTest': model.TestMcpConnection| model.TestMcpExternalTool;
    'paramValues'?: model.TestToolParamValues;
    /**
    * name of mcp server
    */
    'serverName': string;

   "toolType": string;
}

export namespace TestMcpToolDetails {





    export function getJsonObj(obj: TestMcpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getJsonObj(obj) as TestMcpToolDetails, ...{
            
                'config': obj.config ?
                
                
                model.McpToolConfiguration.getJsonObj(obj.config) : undefined,
                'mcpTest': obj.mcpTest ?
                
                
                model.TestMcpOperation.getJsonObj(obj.mcpTest) : undefined,
                'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getJsonObj(obj.paramValues) : undefined,

        }};

        
        
        return jsonObj;
    }
    export const toolType = 'MCP';
    export function getDeserializedJsonObj(obj: TestMcpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getDeserializedJsonObj(obj) as TestMcpToolDetails, ...{
            
                    'config': obj.config ?
                
                
                model.McpToolConfiguration.getDeserializedJsonObj(obj.config) : undefined,
                    'mcpTest': obj.mcpTest ?
                
                
                model.TestMcpOperation.getDeserializedJsonObj(obj.mcpTest) : undefined,
                    'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getDeserializedJsonObj(obj.paramValues) : undefined,

         }};

        
        
        return jsonObj;
    }
}
