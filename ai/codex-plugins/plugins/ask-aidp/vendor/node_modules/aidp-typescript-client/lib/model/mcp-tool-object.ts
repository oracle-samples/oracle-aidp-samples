// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tool exposed by an MCP server.
*/
export interface McpToolObject extends model.McpObject {
    /**
    * representation of the input schema for a tool.
    */
    'inputSchema'?: { [key: string]: any; };

   "objectType": string;
}

export namespace McpToolObject {


    export function getJsonObj(obj: McpToolObject, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.McpObject.getJsonObj(obj) as McpToolObject, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const objectType = 'TOOL';
    export function getDeserializedJsonObj(obj: McpToolObject, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.McpObject.getDeserializedJsonObj(obj) as McpToolObject, ...{
            

         }};

        
        
        return jsonObj;
    }
}
