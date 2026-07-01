// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Specifies the name and value of the output parameter.
*/
export interface OutputParameter {
    /**
    * The name of the parameter defined by user.
    */
    'name': string;
    /**
    * Value of the output parameter.
    */
    'value'?: string;

}

export namespace OutputParameter {



    export function getJsonObj(obj: OutputParameter): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: OutputParameter): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
