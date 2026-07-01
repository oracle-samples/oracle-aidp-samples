// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* span status
*/
export interface SpanStatus {
    /**
    * span status code
    */
    'code'?: string;
    /**
    * span message
    */
    'message'?: string;

}

export namespace SpanStatus {



    export function getJsonObj(obj: SpanStatus): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SpanStatus): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
