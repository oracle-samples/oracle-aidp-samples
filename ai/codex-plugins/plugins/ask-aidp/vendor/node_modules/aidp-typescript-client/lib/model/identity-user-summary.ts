// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A user in the tenancy.
*/
export interface IdentityUserSummary {
    /**
    * The ID of the user.
    */
    'userId'?: string;
    /**
    * The login userName used by the user.
    */
    'userName'?: string;
    /**
    * The email of the user.
    */
    'userEmail'?: string;

}

export namespace IdentityUserSummary {




    export function getJsonObj(obj: IdentityUserSummary): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: IdentityUserSummary): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
