// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response when toolType = SQL.
*/
export interface SqlTestToolResult extends model.TestToolResult {
    'result': model.SqlResult;

   "toolType": string;
}

export namespace SqlTestToolResult {


    export function getJsonObj(obj: SqlTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getJsonObj(obj) as SqlTestToolResult, ...{
            
                'result': obj.result ?
                
                
                model.SqlResult.getJsonObj(obj.result) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'SQL';
    export function getDeserializedJsonObj(obj: SqlTestToolResult, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolResult.getDeserializedJsonObj(obj) as SqlTestToolResult, ...{
            
                    'result': obj.result ?
                
                
                model.SqlResult.getDeserializedJsonObj(obj.result) : undefined,
         }};

        
        
        return jsonObj;
    }
}
