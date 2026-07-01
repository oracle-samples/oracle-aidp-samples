// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Resource exposed by an MCP server.
*/
export interface McpResourceObject extends model.McpObject {
    /**
    * serialized schema of resource persisted within an MCP server.
    */
    'resourceSchema'?: string;

   "objectType": string;
}

export namespace McpResourceObject {


    export function getJsonObj(obj: McpResourceObject, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.McpObject.getJsonObj(obj) as McpResourceObject, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const objectType = 'RESOURCE';
    export function getDeserializedJsonObj(obj: McpResourceObject, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.McpObject.getDeserializedJsonObj(obj) as McpResourceObject, ...{
            

         }};

        
        
        return jsonObj;
    }
}
