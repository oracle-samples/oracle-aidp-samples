// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Tool Node in an Agent Flow
*/
export interface RagToolNode extends model.AgentFlowNode {
    'inputSchema'?: model.RagToolInputSchema;

   "type": string;
}

export namespace RagToolNode {


    export function getJsonObj(obj: RagToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as RagToolNode, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'RAG_TOOL';
    export function getDeserializedJsonObj(obj: RagToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as RagToolNode, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.RagToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
         }};

        
        
        return jsonObj;
    }
}
