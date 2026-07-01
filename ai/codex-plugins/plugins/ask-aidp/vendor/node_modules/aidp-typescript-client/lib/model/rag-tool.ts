// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for RAG Tool
*/
export interface RagTool extends model.Tool {
    'inputSchema'?: model.RagToolInputSchema;
    'toolConfig'?: model.RagToolConfiguration;

   "toolType": string;
}

export namespace RagTool {



    export function getJsonObj(obj: RagTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getJsonObj(obj) as RagTool, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
                'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'RAG';
    export function getDeserializedJsonObj(obj: RagTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Tool.getDeserializedJsonObj(obj) as RagTool, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
                    'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
