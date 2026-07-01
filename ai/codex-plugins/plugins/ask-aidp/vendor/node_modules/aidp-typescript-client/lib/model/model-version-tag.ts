// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* ModelVersion tag.
*/
export interface ModelVersionTag {
    /**
    * Key of the tag.
    */
    'key'?: string;
    /**
    * Value of the tag.
    */
    'value'?: string;

}

export namespace ModelVersionTag {



    export function getJsonObj(obj: ModelVersionTag): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelVersionTag): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
