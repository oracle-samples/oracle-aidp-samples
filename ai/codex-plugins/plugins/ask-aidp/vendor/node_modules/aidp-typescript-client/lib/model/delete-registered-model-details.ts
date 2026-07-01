// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the registered model to delete.
*/
export interface DeleteRegisteredModelDetails {
    /**
    * Name of the registered model.
    */
    'name': string;

}

export namespace DeleteRegisteredModelDetails {


    export function getJsonObj(obj: DeleteRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
