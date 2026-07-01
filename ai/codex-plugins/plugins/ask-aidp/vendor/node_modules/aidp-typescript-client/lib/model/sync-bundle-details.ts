// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for bundle sync.
*/
export interface SyncBundleDetails {
    /**
    * Target folder for the new bundle.
    */
    'path': string;

}

export namespace SyncBundleDetails {


    export function getJsonObj(obj: SyncBundleDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SyncBundleDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
