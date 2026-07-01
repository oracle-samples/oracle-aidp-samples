// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Configuration for a guardrail category
*/
export interface CategoryConfig {
    /**
    * Category
    */
    'category'?: string;
    /**
    * Whether this category is enabled
    */
    'isEnabled'?: boolean;
    /**
    * Threshold value for this category (0.0 to 1.0) Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'threshold'?: number;
    /**
    * Action to take for this category
    */
    'action'?: model.PolicyAction;

}

export namespace CategoryConfig {





    export function getJsonObj(obj: CategoryConfig): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CategoryConfig): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
