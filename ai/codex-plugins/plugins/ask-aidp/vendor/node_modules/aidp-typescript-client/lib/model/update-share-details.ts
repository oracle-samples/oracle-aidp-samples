// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details for updating a Delta Share metadata in AI Data Platform Workbench.
*/
export interface UpdateShareDetails {
    /**
    * A user-friendly name. Has to be unique within the AI Data Platform Workbench instance.
    */
    'displayName'?: string;
    /**
    * A description associated with this share.
    */
    'description'?: string;

}

export namespace UpdateShareDetails {



    export function getJsonObj(obj: UpdateShareDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateShareDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
