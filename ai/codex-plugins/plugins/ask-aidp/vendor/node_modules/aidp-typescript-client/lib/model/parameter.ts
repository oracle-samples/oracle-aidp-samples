// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Specifies the name and value of the defined parameter.
*/
export interface Parameter {
    /**
    * The name of the defined parameter. May only contain alphanumeric characters, '_', '-', and '.'
    */
    'name': string;
    /**
    * Value of the defined parameter.
    */
    'value'?: string;

}

export namespace Parameter {



    export function getJsonObj(obj: Parameter): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Parameter): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
