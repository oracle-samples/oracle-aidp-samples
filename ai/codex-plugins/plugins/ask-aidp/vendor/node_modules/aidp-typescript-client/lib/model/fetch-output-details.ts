// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The payload to fetch TaskRunOutput.
*/
export interface FetchOutputDetails {
    /**
    * A unique identifier for the output.
    */
    'outputKey'?: string;

}

export namespace FetchOutputDetails {


    export function getJsonObj(obj: FetchOutputDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FetchOutputDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
