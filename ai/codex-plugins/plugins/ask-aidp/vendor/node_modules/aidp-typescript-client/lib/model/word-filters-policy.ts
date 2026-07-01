// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Word filters safety policy
*/
export interface WordFiltersPolicy extends model.SafetyPolicy {
    /**
    * List of banned words or regex patterns
    */
    'words'?: Array<string>;
    /**
    * List of regex patterns to filter
    */
    'regexPatterns'?: Array<string>;

   "policyType": string;
}

export namespace WordFiltersPolicy {



    export function getJsonObj(obj: WordFiltersPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getJsonObj(obj) as WordFiltersPolicy, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const policyType = 'WORD_FILTERS';
    export function getDeserializedJsonObj(obj: WordFiltersPolicy, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SafetyPolicy.getDeserializedJsonObj(obj) as WordFiltersPolicy, ...{
            


         }};

        
        
        return jsonObj;
    }
}
