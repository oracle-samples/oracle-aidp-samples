// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result from custom tool execution in MCP format.
*/
export interface CustomResult {
    /**
    * MCP-formatted content blocks
    */
    'content'?: Array<{ [key: string]: any; }>;
    /**
    * Structured result data
    */
    'structuredContent'?: { [key: string]: any; };
    /**
    * Whether the result represents an error
    */
    'isError'?: boolean;
    /**
    * Name of the executed tool
    */
    'toolName'?: string;

}

export namespace CustomResult {





    export function getJsonObj(obj: CustomResult): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CustomResult): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
