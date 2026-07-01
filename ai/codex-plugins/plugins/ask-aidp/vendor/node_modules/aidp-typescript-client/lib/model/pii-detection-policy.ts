// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* PII detection safety policy
*/
export interface PiiDetectionPolicy extends model.SafetyPolicy {
    /**
    * List of PII categories to detect
    */
    'piiCategories'?: Array<model.PiiCategory>;
    /**
    * Custom PII detection rules
    */
    'customPiiRules'?: Array<model.CustomPiiRule>;

   "policyType": string;
}

export namespace PiiDetectionPolicy {



    export function getJsonObj(obj: PiiDetectionPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getJsonObj(obj) as PiiDetectionPolicy, ...{
            
                'piiCategories': obj.piiCategories ?
                
                obj.piiCategories.map((item)=>{return model.PiiCategory.getJsonObj(item)})
                
                 : undefined,
                'customPiiRules': obj.customPiiRules ?
                
                obj.customPiiRules.map((item)=>{return model.CustomPiiRule.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const policyType = 'PII_DETECTION';
    export function getDeserializedJsonObj(obj: PiiDetectionPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getDeserializedJsonObj(obj) as PiiDetectionPolicy, ...{
            
                    'piiCategories': obj.piiCategories ?
                
                obj.piiCategories.map((item)=>{return model.PiiCategory.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'customPiiRules': obj.customPiiRules ?
                
                obj.customPiiRules.map((item)=>{return model.CustomPiiRule.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
