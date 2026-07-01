// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Key of the registered model tag.
*/
export interface RegisteredModelTagKey {
    /**
    * Tag key.
    */
    'key': string;

}

export namespace RegisteredModelTagKey {


    export function getJsonObj(obj: RegisteredModelTagKey): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RegisteredModelTagKey): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
