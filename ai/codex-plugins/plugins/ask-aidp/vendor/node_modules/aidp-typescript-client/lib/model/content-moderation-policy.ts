// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Content moderation safety policy
*/
export interface ContentModerationPolicy extends model.SafetyPolicy {
    /**
    * Content moderation categories and their configurations
    */
    'categories'?: Array<model.ContentModerationCategoryConfig>;

   "policyType": string;
}

export namespace ContentModerationPolicy {


    export function getJsonObj(obj: ContentModerationPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getJsonObj(obj) as ContentModerationPolicy, ...{
            
                'categories': obj.categories ?
                
                obj.categories.map((item)=>{return model.ContentModerationCategoryConfig.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const policyType = 'CONTENT_MODERATION';
    export function getDeserializedJsonObj(obj: ContentModerationPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getDeserializedJsonObj(obj) as ContentModerationPolicy, ...{
            
                    'categories': obj.categories ?
                
                obj.categories.map((item)=>{return model.ContentModerationCategoryConfig.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
