// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for deleting tag of a model version
*/
export interface DeleteModelVersionTagResponseDetails {

}

export namespace DeleteModelVersionTagResponseDetails {

    export function getJsonObj(obj: DeleteModelVersionTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteModelVersionTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
