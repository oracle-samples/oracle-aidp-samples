// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The metadata information.
*/
export interface UpdateFileMetadataDetails {
    /**
    * The metadata information in map form. Example entries include system:key1=val1 and user:key2=val1.
    */
    'metadata'?: { [key: string]: string; };
    /**
    * Action to be taken in case of conflict.
    */
    'action': model.UpdateFileMetadataActionType;

}

export namespace UpdateFileMetadataDetails {



    export function getJsonObj(obj: UpdateFileMetadataDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateFileMetadataDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
