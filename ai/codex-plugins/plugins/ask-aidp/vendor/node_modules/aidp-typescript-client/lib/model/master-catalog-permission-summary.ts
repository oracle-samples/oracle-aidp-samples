// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a Master Catalog Permission.
*/
export interface MasterCatalogPermissionSummary {
    /**
    * The ocid of user/group and name in case of role.
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
    * The selected permissions for a Master Catalog.
    */
    'granteePermissions'?: Array<MasterCatalogPermissionSummary.GranteePermissions>;

}

export namespace MasterCatalogPermissionSummary {




    export enum GranteePermissions {
    
    CreateCatalog = "CREATE_CATALOG",
    Admin = "ADMIN",
    CreateShare = "CREATE_SHARE",
    CreateRecipient = "CREATE_RECIPIENT",
    CreateCredential = "CREATE_CREDENTIAL"

}


    export function getJsonObj(obj: MasterCatalogPermissionSummary): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MasterCatalogPermissionSummary): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
