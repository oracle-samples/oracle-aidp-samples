// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Custom PII detection rule
*/
export interface CustomPiiRule {
    /**
    * Name of the custom rule
    */
    'name': string;
    /**
    * Regex pattern for detection
    */
    'pattern': string;
    /**
    * Prefix pattern to match
    */
    'prefix'?: string;
    /**
    * Suffix pattern to match
    */
    'suffix'?: string;
    /**
    * Whether the pattern is case sensitive
    */
    'isCaseSensitive'?: boolean;
    /**
    * Maximum distance for pattern matching Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxDistance'?: number;
    /**
    * Priority of this rule Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'priority'?: number;

}

export namespace CustomPiiRule {








    export function getJsonObj(obj: CustomPiiRule): object {
        const jsonObj = {...obj, ...{
            







        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CustomPiiRule): object {
        const jsonObj = {...obj, ...{
            







         }};

        
        
        return jsonObj;
    }
}
