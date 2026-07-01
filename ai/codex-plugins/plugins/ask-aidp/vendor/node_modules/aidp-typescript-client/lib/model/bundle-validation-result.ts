// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of validating bundle structure/resources. Contains status and lists of error or warning messages.
* 
*/
export interface BundleValidationResult {
    /**
    * Validation status.
    */
    'status': BundleValidationResult.Status;
    /**
    * List of validation error messages.
    */
    'errors'?: Array<string>;
    /**
    * List of validation warning messages.
    */
    'warnings'?: Array<string>;

}

export namespace BundleValidationResult {

    export enum Status {
    
    Valid = "VALID",
    Invalid = "INVALID",
    Warnings = "WARNINGS"

}




    export function getJsonObj(obj: BundleValidationResult): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: BundleValidationResult): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
