// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The list of allowed tool names allowed on an MCP server.
*/
export interface AllowedToolDetails {
    /**
    * custom instruction for tool
    */
    'instruction'?: string;
    /**
    * Map of parameter names to their default string values.
    */
    'argOverrides'?: { [key: string]: string; };
    'tool': model.McpToolObject;

}

export namespace AllowedToolDetails {




    export function getJsonObj(obj: AllowedToolDetails): object {
        const jsonObj = {...obj, ...{
            


                'tool': obj.tool ?
                
                
                model.McpToolObject.getJsonObj(obj.tool) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AllowedToolDetails): object {
        const jsonObj = {...obj, ...{
            


                    'tool': obj.tool ?
                
                
                model.McpToolObject.getDeserializedJsonObj(obj.tool) : undefined,
         }};

        
        
        return jsonObj;
    }
}
