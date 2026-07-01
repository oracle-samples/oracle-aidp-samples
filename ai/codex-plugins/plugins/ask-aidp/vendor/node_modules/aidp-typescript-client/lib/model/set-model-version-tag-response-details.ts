// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for setting tag of a model version
*/
export interface SetModelVersionTagResponseDetails {

}

export namespace SetModelVersionTagResponseDetails {

    export function getJsonObj(obj: SetModelVersionTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SetModelVersionTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
