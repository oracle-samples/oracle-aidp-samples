// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a share permission.
*/
export interface SharePermissionSummary {
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
    * The selected permissions for a share.
    */
    'granteePermissions': Array<SharePermissionSummary.GranteePermissions>;

}

export namespace SharePermissionSummary {




    export enum GranteePermissions {
    
    Admin = "ADMIN",
    Read = "READ",
    Use = "USE",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: SharePermissionSummary): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SharePermissionSummary): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
