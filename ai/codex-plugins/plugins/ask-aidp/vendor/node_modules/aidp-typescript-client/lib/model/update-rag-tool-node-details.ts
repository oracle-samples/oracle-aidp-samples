// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a RAG Tool
*/
export interface UpdateRagToolNodeDetails extends model.UpdateAgentFlowNodeDetails {
    'inputSchema'?: model.RagToolInputSchema;

   "type": string;
}

export namespace UpdateRagToolNodeDetails {


    export function getJsonObj(obj: UpdateRagToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getJsonObj(obj) as UpdateRagToolNodeDetails, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'RAG_TOOL';
    export function getDeserializedJsonObj(obj: UpdateRagToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as UpdateRagToolNodeDetails, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
         }};

        
        
        return jsonObj;
    }
}
