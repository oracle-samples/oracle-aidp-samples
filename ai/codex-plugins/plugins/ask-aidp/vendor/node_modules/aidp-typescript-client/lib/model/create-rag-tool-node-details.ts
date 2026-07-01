// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a RAG Tool Node in an Agent Flow
*/
export interface CreateRagToolNodeDetails extends model.CreateAgentFlowNodeDetails {
    /**
    * The unique identifier (key) of the saved AI tool
    */
    'toolKey'?: string;
    'inputSchema'?: model.RagToolInputSchema;
    'toolConfig'?: model.RagToolConfiguration;

   "type": string;
}

export namespace CreateRagToolNodeDetails {




    export function getJsonObj(obj: CreateRagToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getJsonObj(obj) as CreateRagToolNodeDetails, ...{
            

                'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
                'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'RAG_TOOL';
    export function getDeserializedJsonObj(obj: CreateRagToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as CreateRagToolNodeDetails, ...{
            

                    'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
                    'toolConfig': obj.toolConfig ?
                
                
                model.RagToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
