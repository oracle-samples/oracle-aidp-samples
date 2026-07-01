// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run param.
*/
export interface ExperimentRunParam {
    /**
    * Key of the parameter.
    */
    'key'?: string;
    /**
    * Value of the parameter.
    */
    'value'?: string;

}

export namespace ExperimentRunParam {



    export function getJsonObj(obj: ExperimentRunParam): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunParam): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
