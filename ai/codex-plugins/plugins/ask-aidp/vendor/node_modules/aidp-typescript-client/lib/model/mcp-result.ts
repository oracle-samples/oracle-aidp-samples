// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Generated text info for mcp result.
*/
export interface McpResult {
    /**
    * The generated content from the mcp.
    */
    'data': string;

}

export namespace McpResult {


    export function getJsonObj(obj: McpResult): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: McpResult): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
