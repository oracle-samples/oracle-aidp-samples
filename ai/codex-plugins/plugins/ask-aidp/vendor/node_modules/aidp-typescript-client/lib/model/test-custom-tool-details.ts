// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request details when toolType = CUSTOM
*/
export interface TestCustomToolDetails extends model.TestToolDetails {
    'toolConfig': model.CustomToolConfiguration;
    'paramValues'?: model.TestToolParamValues;
    /**
    * Base64-encoded ZIP file content for inline package upload during testing
    */
    'packageContent'?: string;

   "toolType": string;
}

export namespace TestCustomToolDetails {




    export function getJsonObj(obj: TestCustomToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getJsonObj(obj) as TestCustomToolDetails, ...{
            
                'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
                'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getJsonObj(obj.paramValues) : undefined,

        }};

        
        
        return jsonObj;
    }
    export const toolType = 'CUSTOM';
    export function getDeserializedJsonObj(obj: TestCustomToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getDeserializedJsonObj(obj) as TestCustomToolDetails, ...{
            
                    'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
                    'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getDeserializedJsonObj(obj.paramValues) : undefined,

         }};

        
        
        return jsonObj;
    }
}
