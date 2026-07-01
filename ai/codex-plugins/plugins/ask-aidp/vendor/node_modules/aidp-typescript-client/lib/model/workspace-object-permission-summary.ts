// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a workspace object permission.
*/
export interface WorkspaceObjectPermissionSummary {
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
    * The selected permissions for a workspace object.
    */
    'granteePermissions': Array<WorkspaceObjectPermissionSummary.GranteePermissions>;
    /**
    * Property to determine if permission is inheritable or not. This is applicable only on workspace folders not on files.
    */
    'isPermissionsInheritable'?: boolean;

}

export namespace WorkspaceObjectPermissionSummary {




    export enum GranteePermissions {
    
    Read = "READ",
    Use = "USE",
    Manage = "MANAGE",
    Admin = "ADMIN",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}



    export function getJsonObj(obj: WorkspaceObjectPermissionSummary): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceObjectPermissionSummary): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
