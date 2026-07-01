// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request details when toolType = SQL.
*/
export interface TestSqlToolDetails extends model.TestToolDetails {
    'config': model.SqlToolConfiguration;
    'paramValues': model.TestToolParamValues;

   "toolType": string;
}

export namespace TestSqlToolDetails {



    export function getJsonObj(obj: TestSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getJsonObj(obj) as TestSqlToolDetails, ...{
            
                'config': obj.config ?
                
                
                model.SqlToolConfiguration.getJsonObj(obj.config) : undefined,
                'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getJsonObj(obj.paramValues) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'SQL';
    export function getDeserializedJsonObj(obj: TestSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getDeserializedJsonObj(obj) as TestSqlToolDetails, ...{
            
                    'config': obj.config ?
                
                
                model.SqlToolConfiguration.getDeserializedJsonObj(obj.config) : undefined,
                    'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getDeserializedJsonObj(obj.paramValues) : undefined,
         }};

        
        
        return jsonObj;
    }
}
