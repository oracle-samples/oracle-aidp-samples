// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a volume.
*/
export interface UpdateVolumeDetails {
    /**
    * A user-friendly name. Has to be unique within the schema and is changeable.
    */
    'displayName'?: string;
    /**
    * Short description of the volume
    */
    'description'?: string;

}

export namespace UpdateVolumeDetails {



    export function getJsonObj(obj: UpdateVolumeDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateVolumeDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
