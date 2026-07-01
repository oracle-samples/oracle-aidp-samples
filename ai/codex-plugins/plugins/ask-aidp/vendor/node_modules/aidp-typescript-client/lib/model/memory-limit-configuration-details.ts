// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Configuration details for memory limits.
*/
export interface MemoryLimitConfigurationDetails {
    /**
    * Message-count limit for truncation middleware. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'messageLimit'?: number;
    /**
    * Approximate token-count limit for truncation middleware. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'tokenLimit'?: number;

}

export namespace MemoryLimitConfigurationDetails {



    export function getJsonObj(obj: MemoryLimitConfigurationDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MemoryLimitConfigurationDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
