// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Configuration for a content moderation category
*/
export interface ContentModerationCategoryConfig {
    /**
    * Content moderation category
    */
    'category': model.ContentModerationCategory;
    /**
    * Whether this category is enabled
    */
    'isEnabled': boolean;
    /**
    * Threshold value for this category (0.0 to 1.0) Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'threshold': number;
    /**
    * Action to take for this category
    */
    'action': model.PolicyAction;

}

export namespace ContentModerationCategoryConfig {





    export function getJsonObj(obj: ContentModerationCategoryConfig): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ContentModerationCategoryConfig): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
