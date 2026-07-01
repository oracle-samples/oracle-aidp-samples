// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for bundle deploy.
*/
export interface DeployBundleDetails {
    /**
    * Target folder for the new bundle.
    */
    'path': string;

}

export namespace DeployBundleDetails {


    export function getJsonObj(obj: DeployBundleDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeployBundleDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
