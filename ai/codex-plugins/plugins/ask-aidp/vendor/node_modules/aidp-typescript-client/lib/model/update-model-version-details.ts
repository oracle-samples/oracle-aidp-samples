// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the model version.
*/
export interface UpdateModelVersionDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Model version number.
    */
    'version': string;
    /**
    * New description for the model version.
    */
    'description'?: string;

}

export namespace UpdateModelVersionDetails {




    export function getJsonObj(obj: UpdateModelVersionDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateModelVersionDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
