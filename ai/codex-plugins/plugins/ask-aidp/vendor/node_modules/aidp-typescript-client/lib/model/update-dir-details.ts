// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a folder.
*/
export interface UpdateDirDetails {
    /**
    * The name of the volume folder. This will be the name of the folder in the volume.
* 
    */
    'displayName'?: string;

}

export namespace UpdateDirDetails {


    export function getJsonObj(obj: UpdateDirDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateDirDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
