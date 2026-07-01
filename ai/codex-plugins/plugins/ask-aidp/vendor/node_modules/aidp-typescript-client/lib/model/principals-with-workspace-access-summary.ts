// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* This is a object containing the user and the type of permission they have
* 
*/
export interface PrincipalsWithWorkspaceAccessSummary {
    /**
    * The OCID of user/group and name in case of role.
    */
    'grantee': string;
    /**
    * The simplified name of the grantee.
    */
    'granteeName': string;
    /**
    * The type of grantee.
    */
    'granteeType': model.GranteeType;

}

export namespace PrincipalsWithWorkspaceAccessSummary {




    export function getJsonObj(obj: PrincipalsWithWorkspaceAccessSummary): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PrincipalsWithWorkspaceAccessSummary): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
