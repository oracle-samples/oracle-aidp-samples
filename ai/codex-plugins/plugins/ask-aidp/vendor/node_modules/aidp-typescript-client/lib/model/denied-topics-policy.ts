// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Denied topics safety policy
*/
export interface DeniedTopicsPolicy extends model.SafetyPolicy {
    /**
    * List of denied topics
    */
    'topics'?: Array<model.DeniedTopic>;

   "policyType": string;
}

export namespace DeniedTopicsPolicy {


    export function getJsonObj(obj: DeniedTopicsPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getJsonObj(obj) as DeniedTopicsPolicy, ...{
            
                'topics': obj.topics ?
                
                obj.topics.map((item)=>{return model.DeniedTopic.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const policyType = 'DENIED_TOPICS';
    export function getDeserializedJsonObj(obj: DeniedTopicsPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getDeserializedJsonObj(obj) as DeniedTopicsPolicy, ...{
            
                    'topics': obj.topics ?
                
                obj.topics.map((item)=>{return model.DeniedTopic.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
