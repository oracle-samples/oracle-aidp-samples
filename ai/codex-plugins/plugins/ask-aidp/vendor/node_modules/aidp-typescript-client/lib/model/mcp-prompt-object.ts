// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Prompt exposed by an MCP server.
*/
export interface McpPromptObject extends model.McpObject {
    /**
    * prompt persisted within an MCP server.
    */
    'prompt'?: string;

   "objectType": string;
}

export namespace McpPromptObject {


    export function getJsonObj(obj: McpPromptObject, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.McpObject.getJsonObj(obj) as McpPromptObject, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const objectType = 'PROMPT';
    export function getDeserializedJsonObj(obj: McpPromptObject, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.McpObject.getDeserializedJsonObj(obj) as McpPromptObject, ...{
            

         }};

        
        
        return jsonObj;
    }
}
