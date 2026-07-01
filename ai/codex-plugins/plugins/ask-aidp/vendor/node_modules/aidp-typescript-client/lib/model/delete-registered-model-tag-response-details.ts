// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for deleting tag of a registered model
*/
export interface DeleteRegisteredModelTagResponseDetails {

}

export namespace DeleteRegisteredModelTagResponseDetails {

    export function getJsonObj(obj: DeleteRegisteredModelTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteRegisteredModelTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
