// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run tag.
*/
export interface InputTag {
    /**
    * Key of the tag.
    */
    'key': string;
    /**
    * Value of the tag.
    */
    'value': string;

}

export namespace InputTag {



    export function getJsonObj(obj: InputTag): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: InputTag): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
