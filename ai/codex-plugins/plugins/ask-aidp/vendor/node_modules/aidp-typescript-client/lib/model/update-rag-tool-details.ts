// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a RAG Tool
*/
export interface UpdateRagToolDetails extends model.UpdateToolDetails {
    'inputSchema'?: model.RagToolInputSchema;
    'toolConfig'?: model.RagToolConfiguration;

   "toolType": string;
}

export namespace UpdateRagToolDetails {



    export function getJsonObj(obj: UpdateRagToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getJsonObj(obj) as UpdateRagToolDetails, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
                'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'RAG';
    export function getDeserializedJsonObj(obj: UpdateRagToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getDeserializedJsonObj(obj) as UpdateRagToolDetails, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
                    'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
