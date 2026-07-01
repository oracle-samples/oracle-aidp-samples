// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a workspace permission.
*/
export interface WorkspacePermissionSummary {
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
    * The selected permissions for a workspace.
    */
    'granteePermissions': Array<WorkspacePermissionSummary.GranteePermissions>;

}

export namespace WorkspacePermissionSummary {




    export enum GranteePermissions {
    
    User = "USER",
    PrivilegedUser = "PRIVILEGED_USER",
    Administrator = "ADMINISTRATOR",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: WorkspacePermissionSummary): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspacePermissionSummary): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
