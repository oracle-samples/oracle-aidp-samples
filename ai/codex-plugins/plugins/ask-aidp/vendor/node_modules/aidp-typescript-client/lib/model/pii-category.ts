// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* PII category configuration
*/
export interface PiiCategory {
    /**
    * PII category name (e.g., SSN, EMAIL, PHONE_NUMBER)
    */
    'category': model.PiiCategoryType;
    /**
    * Whether this category is enabled
    */
    'isEnabled': boolean;
    /**
    * Action to take for this category
    */
    'action': model.PolicyAction;
    /**
    * Detection threshold for this category Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'threshold'?: number;

}

export namespace PiiCategory {





    export function getJsonObj(obj: PiiCategory): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PiiCategory): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
