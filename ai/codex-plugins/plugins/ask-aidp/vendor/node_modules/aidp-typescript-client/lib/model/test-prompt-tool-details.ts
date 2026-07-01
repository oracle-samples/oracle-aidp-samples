// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request details when toolType = PROMPT.
*/
export interface TestPromptToolDetails extends model.TestToolDetails {
    'config': model.PromptToolConfiguration;
    'paramValues': model.TestToolParamValues;

   "toolType": string;
}

export namespace TestPromptToolDetails {



    export function getJsonObj(obj: TestPromptToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getJsonObj(obj) as TestPromptToolDetails, ...{
            
                'config': obj.config ?
                
                
                model.PromptToolConfiguration.getJsonObj(obj.config) : undefined,
                'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getJsonObj(obj.paramValues) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'PROMPT';
    export function getDeserializedJsonObj(obj: TestPromptToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getDeserializedJsonObj(obj) as TestPromptToolDetails, ...{
            
                    'config': obj.config ?
                
                
                model.PromptToolConfiguration.getDeserializedJsonObj(obj.config) : undefined,
                    'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getDeserializedJsonObj(obj.paramValues) : undefined,
         }};

        
        
        return jsonObj;
    }
}
