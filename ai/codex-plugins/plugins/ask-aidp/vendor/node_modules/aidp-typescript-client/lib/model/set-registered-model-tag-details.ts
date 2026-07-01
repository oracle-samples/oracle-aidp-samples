// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the registered model tag.
*/
export interface SetRegisteredModelTagDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Tag key.
    */
    'key': string;
    /**
    * Tag value.
    */
    'value': string;

}

export namespace SetRegisteredModelTagDetails {




    export function getJsonObj(obj: SetRegisteredModelTagDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SetRegisteredModelTagDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
