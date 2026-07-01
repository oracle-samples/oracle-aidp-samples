// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Validation error for a component in agent flow diagram.
*/
export interface ValidationError {
    /**
    * Type of error.
    */
    'type': string;
    /**
    * Error message.
    */
    'message': string;
    /**
    * Error name
    */
    'name': string;
    /**
    * Id of the component that caused the error
    */
    'key': string;

}

export namespace ValidationError {





    export function getJsonObj(obj: ValidationError): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ValidationError): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
