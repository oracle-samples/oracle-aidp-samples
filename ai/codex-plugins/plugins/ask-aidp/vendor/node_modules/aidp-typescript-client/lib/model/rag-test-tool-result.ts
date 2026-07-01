// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response when toolType = RAG.
*/
export interface RagTestToolResult extends model.TestToolResult {
    'result': model.RagResult;

   "toolType": string;
}

export namespace RagTestToolResult {


    export function getJsonObj(obj: RagTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getJsonObj(obj) as RagTestToolResult, ...{
            
                'result': obj.result ?
                
                
                model.RagResult.getJsonObj(obj.result) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'RAG';
    export function getDeserializedJsonObj(obj: RagTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getDeserializedJsonObj(obj) as RagTestToolResult, ...{
            
                    'result': obj.result ?
                
                
                model.RagResult.getDeserializedJsonObj(obj.result) : undefined,
         }};

        
        
        return jsonObj;
    }
}
