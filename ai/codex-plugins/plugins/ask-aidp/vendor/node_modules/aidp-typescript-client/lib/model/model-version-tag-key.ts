// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Key of the model version tag.
*/
export interface ModelVersionTagKey {
    /**
    * Tag key.
    */
    'key': string;

}

export namespace ModelVersionTagKey {


    export function getJsonObj(obj: ModelVersionTagKey): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelVersionTagKey): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
