// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the model version tag.
*/
export interface SetModelVersionTagDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Model version number.
    */
    'version': string;
    /**
    * Tag key.
    */
    'key': string;
    /**
    * Tag value.
    */
    'value': string;

}

export namespace SetModelVersionTagDetails {





    export function getJsonObj(obj: SetModelVersionTagDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SetModelVersionTagDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
