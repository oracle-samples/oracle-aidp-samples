// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for HTTP Tool
*/
export interface HttpTool extends model.Tool {
    /**
    * The list of template variable properties in the inputSchema
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.HttpToolConfiguration;

   "toolType": string;
}

export namespace HttpTool {



    export function getJsonObj(obj: HttpTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getJsonObj(obj) as HttpTool, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'HTTP';
    export function getDeserializedJsonObj(obj: HttpTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getDeserializedJsonObj(obj) as HttpTool, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
