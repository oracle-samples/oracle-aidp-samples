// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response when toolType = PROMPT.
*/
export interface PromptTestToolResult extends model.TestToolResult {
    'result': model.PromptResult;

   "toolType": string;
}

export namespace PromptTestToolResult {


    export function getJsonObj(obj: PromptTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getJsonObj(obj) as PromptTestToolResult, ...{
            
                'result': obj.result ?
                
                
                model.PromptResult.getJsonObj(obj.result) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'PROMPT';
    export function getDeserializedJsonObj(obj: PromptTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getDeserializedJsonObj(obj) as PromptTestToolResult, ...{
            
                    'result': obj.result ?
                
                
                model.PromptResult.getDeserializedJsonObj(obj.result) : undefined,
         }};

        
        
        return jsonObj;
    }
}
