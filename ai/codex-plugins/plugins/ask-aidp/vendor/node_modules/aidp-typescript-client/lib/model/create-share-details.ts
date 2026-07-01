// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details for creating a Delta Share in AI Data Platform Workbench.
*/
export interface CreateShareDetails {
    /**
    * A user-friendly name. Has to be unique within the AI Data Platform Workbench instance.
    */
    'displayName': string;
    /**
    * A description associated with this share.
    */
    'description'?: string;

}

export namespace CreateShareDetails {



    export function getJsonObj(obj: CreateShareDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateShareDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
