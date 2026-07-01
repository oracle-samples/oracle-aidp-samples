// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A group in the tenancy.
*/
export interface IdentityGroupSummary {
    /**
    * The ID of the group.
    */
    'groupId'?: string;
    /**
    * The name of the group.
    */
    'groupName'?: string;

}

export namespace IdentityGroupSummary {



    export function getJsonObj(obj: IdentityGroupSummary): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: IdentityGroupSummary): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
