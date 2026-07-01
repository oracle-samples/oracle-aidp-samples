// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tag for the registered model.
*/
export interface RegisteredModelTag {
    /**
    * Key of the registered model tag.
    */
    'key'?: string;
    /**
    * Value of the registered model tag.
    */
    'value'?: string;

}

export namespace RegisteredModelTag {



    export function getJsonObj(obj: RegisteredModelTag): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RegisteredModelTag): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
