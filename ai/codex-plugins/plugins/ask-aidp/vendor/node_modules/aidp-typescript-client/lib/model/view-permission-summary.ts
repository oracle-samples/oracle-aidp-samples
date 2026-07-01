// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a view permission.
*/
export interface ViewPermissionSummary {
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
    * The selected permissions for a view.
    */
    'granteePermissions': Array<ViewPermissionSummary.GranteePermissions>;
    /**
    * The list of the columns included for permission assignment.
    */
    'columns'?: Array<string>;
    /**
    * The list of the columns excluded from permission assignment.
    */
    'excludedColumns'?: Array<string>;
    /**
    * The permission listed is inherited or not from object up in hierarchy.
    */
    'isInherited'?: boolean;
    /**
    * Name of the object to which this permission belong to. This would be the name of view if permission is not inherited or name of object up in hierarchy if permission is inherited.
    */
    'resourceName'?: string;

}

export namespace ViewPermissionSummary {




    export enum GranteePermissions {
    
    Select = "SELECT",
    Manage = "MANAGE",
    Write = "WRITE",
    Insert = "INSERT",
    Update = "UPDATE",
    Delete = "DELETE",
    Alter = "ALTER",
    Admin = "ADMIN",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}






    export function getJsonObj(obj: ViewPermissionSummary): object {
        const jsonObj = {...obj, ...{
            








        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ViewPermissionSummary): object {
        const jsonObj = {...obj, ...{
            








         }};

        
        
        return jsonObj;
    }
}
