// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for tagging a registered model.
*/
export interface SetRegisteredModelTagResponseDetails {

}

export namespace SetRegisteredModelTagResponseDetails {

    export function getJsonObj(obj: SetRegisteredModelTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SetRegisteredModelTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
