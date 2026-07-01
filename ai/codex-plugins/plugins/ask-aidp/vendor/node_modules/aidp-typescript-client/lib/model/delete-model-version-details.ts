// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the model version to delete.
*/
export interface DeleteModelVersionDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Model version number.
    */
    'version': string;

}

export namespace DeleteModelVersionDetails {



    export function getJsonObj(obj: DeleteModelVersionDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteModelVersionDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
