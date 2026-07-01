// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single tool class entry within a custom tool package
*/
export interface CustomToolEntry {
    /**
    * Python class name (e.g., WeatherTool, BashTool)
    */
    'toolClassName': string;
    /**
    * Human-readable tool name shown to LLM
    */
    'displayName': string;
    /**
    * Tool description for LLM tool selection
    */
    'description'?: string;
    /**
    * Tool version
    */
    'version'?: string;
    /**
    * Tool-specific configuration values (supports template variables)
    */
    'config'?: { [key: string]: any; };
    /**
    * JSON schema for tool input parameters
    */
    'inputSchema'?: { [key: string]: any; };

}

export namespace CustomToolEntry {







    export function getJsonObj(obj: CustomToolEntry): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CustomToolEntry): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
