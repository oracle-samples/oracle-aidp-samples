// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response when toolType = HTTP.
*/
export interface HttpTestToolResult extends model.TestToolResult {
    'result': model.HttpResult;

   "toolType": string;
}

export namespace HttpTestToolResult {


    export function getJsonObj(obj: HttpTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getJsonObj(obj) as HttpTestToolResult, ...{
            
                'result': obj.result ?
                
                
                model.HttpResult.getJsonObj(obj.result) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'HTTP';
    export function getDeserializedJsonObj(obj: HttpTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getDeserializedJsonObj(obj) as HttpTestToolResult, ...{
            
                    'result': obj.result ?
                
                
                model.HttpResult.getDeserializedJsonObj(obj.result) : undefined,
         }};

        
        
        return jsonObj;
    }
}
