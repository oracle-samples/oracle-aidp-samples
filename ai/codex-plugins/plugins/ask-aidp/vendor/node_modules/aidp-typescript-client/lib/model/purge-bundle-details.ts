// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for bundle purge.
*/
export interface PurgeBundleDetails {
    /**
    * Target folder for the new bundle.
    */
    'path': string;

}

export namespace PurgeBundleDetails {


    export function getJsonObj(obj: PurgeBundleDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PurgeBundleDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
