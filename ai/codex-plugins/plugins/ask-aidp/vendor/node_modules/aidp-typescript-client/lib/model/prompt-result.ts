// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Generated text info for prompt result.
*/
export interface PromptResult {
    /**
    * Format of the prompt result (e.g., \"text\", \"markdown\", \"image\").
    */
    'type': string;
    /**
    * The generated content from the prompt.
    */
    'data': string;

}

export namespace PromptResult {



    export function getJsonObj(obj: PromptResult): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PromptResult): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
