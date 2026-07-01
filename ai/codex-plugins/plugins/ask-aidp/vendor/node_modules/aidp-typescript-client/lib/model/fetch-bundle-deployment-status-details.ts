// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for fetch BundleDeploymentStatus.
*/
export interface FetchBundleDeploymentStatusDetails {
    /**
    * Target folder for the new bundle.
    */
    'path': string;

}

export namespace FetchBundleDeploymentStatusDetails {


    export function getJsonObj(obj: FetchBundleDeploymentStatusDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FetchBundleDeploymentStatusDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
