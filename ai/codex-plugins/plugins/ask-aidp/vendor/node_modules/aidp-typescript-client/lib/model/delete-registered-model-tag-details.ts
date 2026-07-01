// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the registered model tag.
*/
export interface DeleteRegisteredModelTagDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Tag key.
    */
    'key': string;

}

export namespace DeleteRegisteredModelTagDetails {



    export function getJsonObj(obj: DeleteRegisteredModelTagDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteRegisteredModelTagDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
