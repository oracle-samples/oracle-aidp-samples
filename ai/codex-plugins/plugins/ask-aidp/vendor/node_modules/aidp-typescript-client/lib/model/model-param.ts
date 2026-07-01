// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Model param.
*/
export interface ModelParam {
    /**
    * Name of the parameter.
    */
    'name'?: string;
    /**
    * Value of the parameter.
    */
    'value'?: string;

}

export namespace ModelParam {



    export function getJsonObj(obj: ModelParam): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelParam): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
