// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a credential permission.
*/
export interface PermissionSummary {
    /**
    * The OCID of user/group and name in case of role.
    */
    'grantee': string;
    /**
    * The simplified name of the grantee.
    */
    'granteeName'?: string;
    /**
    * The type of grantee.
    */
    'granteeType': model.GranteeType;
    /**
    * The selected permissions for a credential.
    */
    'granteePermissions': Array<PermissionSummary.GranteePermissions>;
    /**
    * The permission listed is inherited or not from object up in hierarchy.
    */
    'isInherited'?: boolean;
    /**
    * Name of the object to which this permission belongs.
    */
    'resourceName'?: string;

}

export namespace PermissionSummary {




    export enum GranteePermissions {
    
    Use = "USE",
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"

}




    export function getJsonObj(obj: PermissionSummary): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PermissionSummary): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
