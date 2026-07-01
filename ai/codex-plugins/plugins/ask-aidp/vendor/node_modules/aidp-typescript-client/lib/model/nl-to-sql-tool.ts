// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for Natural Language (NL) to SQL Tool
*/
export interface NlToSqlTool extends model.Tool {
    'inputSchema': model.NlToSqlToolInputSchema;
    'toolConfig': model.NlToSqlToolConfiguration;

   "toolType": string;
}

export namespace NlToSqlTool {



    export function getJsonObj(obj: NlToSqlTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getJsonObj(obj) as NlToSqlTool, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.NlToSqlToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
                'toolConfig': obj.toolConfig ?
                
                
                model.NlToSqlToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'NL2SQL';
    export function getDeserializedJsonObj(obj: NlToSqlTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getDeserializedJsonObj(obj) as NlToSqlTool, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.NlToSqlToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
                    'toolConfig': obj.toolConfig ?
                
                
                model.NlToSqlToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
