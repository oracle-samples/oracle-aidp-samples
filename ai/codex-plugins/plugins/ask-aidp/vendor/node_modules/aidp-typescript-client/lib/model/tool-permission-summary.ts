// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a tool permission.
*/
export interface ToolPermissionSummary {
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
    * The selected permissions for a tool.
    */
    'granteePermissions': Array<ToolPermissionSummary.GranteePermissions>;
    /**
    * The list of the columns included for permission assignment.
    */
    'columns'?: Array<string>;
    /**
    * The list of the columns excluded from permission assignment.
    */
    'excludeColumns'?: Array<string>;
    /**
    * If the permission listed is inherited or not from object higher up in hierarchy.
    */
    'isInherited'?: boolean;
    /**
    * Name of the object to which this permission belong to. Name of tool if permission is not inherited or name of object higher up in hierarchy if permission is inherited.
    */
    'resourceName'?: string;

}

export namespace ToolPermissionSummary {




    export enum GranteePermissions {
    
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"

}






    export function getJsonObj(obj: ToolPermissionSummary): object {
        const jsonObj = {...obj, ...{
            








        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ToolPermissionSummary): object {
        const jsonObj = {...obj, ...{
            








         }};

        
        
        return jsonObj;
    }
}
