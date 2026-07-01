// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for fetching bundle overrides.
*/
export interface GetBundleOverridesDetails {
    /**
    * Target folder for the new bundle.
    */
    'path': string;

}

export namespace GetBundleOverridesDetails {


    export function getJsonObj(obj: GetBundleOverridesDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GetBundleOverridesDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
