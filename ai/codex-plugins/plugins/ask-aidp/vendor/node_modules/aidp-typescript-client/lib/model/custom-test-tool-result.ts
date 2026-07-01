// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response when toolType = CUSTOM.
*/
export interface CustomTestToolResult extends model.TestToolResult {
    'result': model.CustomResult;

   "toolType": string;
}

export namespace CustomTestToolResult {


    export function getJsonObj(obj: CustomTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getJsonObj(obj) as CustomTestToolResult, ...{
            
                'result': obj.result ?
                
                
                model.CustomResult.getJsonObj(obj.result) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'CUSTOM';
    export function getDeserializedJsonObj(obj: CustomTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getDeserializedJsonObj(obj) as CustomTestToolResult, ...{
            
                    'result': obj.result ?
                
                
                model.CustomResult.getDeserializedJsonObj(obj.result) : undefined,
         }};

        
        
        return jsonObj;
    }
}
