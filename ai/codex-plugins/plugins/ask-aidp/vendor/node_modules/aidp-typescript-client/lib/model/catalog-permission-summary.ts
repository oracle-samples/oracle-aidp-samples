// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a Catalog Permission.
*/
export interface CatalogPermissionSummary {
    /**
    * The OCID of user/group and name in case of role.
    */
    'grantee'?: string;
    /**
    * The simplified name of the grantee.
    */
    'granteeName'?: string;
    /**
    * The type of grantee.
    */
    'granteeType'?: model.GranteeType;
    /**
    * The selected permissions for a catalog.
    */
    'granteePermissions'?: Array<CatalogPermissionSummary.GranteePermissions>;

}

export namespace CatalogPermissionSummary {




    export enum GranteePermissions {
    
    Select = "SELECT",
    Manage = "MANAGE",
    CreateSchema = "CREATE_SCHEMA",
    Admin = "ADMIN",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: CatalogPermissionSummary): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CatalogPermissionSummary): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
