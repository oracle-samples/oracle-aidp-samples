// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for bundle validate.
*/
export interface ValidateBundleDetails {
    /**
    * Target folder for the new bundle.
    */
    'path': string;

}

export namespace ValidateBundleDetails {


    export function getJsonObj(obj: ValidateBundleDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ValidateBundleDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
