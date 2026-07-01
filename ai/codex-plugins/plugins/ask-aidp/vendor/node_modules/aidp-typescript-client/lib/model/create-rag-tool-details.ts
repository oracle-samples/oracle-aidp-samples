// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a SQL Tool
*/
export interface CreateRagToolDetails extends model.CreateToolDetails {
    'inputSchema'?: model.RagToolInputSchema;
    'toolConfig'?: model.RagToolConfiguration;

   "toolType": string;
}

export namespace CreateRagToolDetails {



    export function getJsonObj(obj: CreateRagToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getJsonObj(obj) as CreateRagToolDetails, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
                'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'RAG';
    export function getDeserializedJsonObj(obj: CreateRagToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getDeserializedJsonObj(obj) as CreateRagToolDetails, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
                    'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
