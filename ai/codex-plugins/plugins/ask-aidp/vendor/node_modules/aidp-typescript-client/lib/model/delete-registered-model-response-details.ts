// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for deleting a registered model.
*/
export interface DeleteRegisteredModelResponseDetails {

}

export namespace DeleteRegisteredModelResponseDetails {

    export function getJsonObj(obj: DeleteRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
