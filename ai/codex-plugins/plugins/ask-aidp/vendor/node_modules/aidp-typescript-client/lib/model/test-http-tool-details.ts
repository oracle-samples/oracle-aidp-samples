// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request details when toolType = HTTP
*/
export interface TestHttpToolDetails extends model.TestToolDetails {
    'config': model.HttpToolConfiguration;
    'paramValues'?: model.TestToolParamValues;

   "toolType": string;
}

export namespace TestHttpToolDetails {



    export function getJsonObj(obj: TestHttpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getJsonObj(obj) as TestHttpToolDetails, ...{
            
                'config': obj.config ?
                
                
                model.HttpToolConfiguration.getJsonObj(obj.config) : undefined,
                'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getJsonObj(obj.paramValues) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'HTTP';
    export function getDeserializedJsonObj(obj: TestHttpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getDeserializedJsonObj(obj) as TestHttpToolDetails, ...{
            
                    'config': obj.config ?
                
                
                model.HttpToolConfiguration.getDeserializedJsonObj(obj.config) : undefined,
                    'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getDeserializedJsonObj(obj.paramValues) : undefined,
         }};

        
        
        return jsonObj;
    }
}
