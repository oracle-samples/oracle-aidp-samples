// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a volume permission.
*/
export interface VolumePermissionSummary {
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
    * The selected permissions for a volume.
    */
    'granteePermissions': Array<VolumePermissionSummary.GranteePermissions>;
    /**
    * The permission listed is inherited or not from object up in hierarchy.
    */
    'isInherited'?: boolean;
    /**
    * Name of the object to which this permission belongs to. This would be the name of a table if permission is not inherited or name of the object up in hierarchy if permission is inherited.
    */
    'resourceName'?: string;

}

export namespace VolumePermissionSummary {




    export enum GranteePermissions {
    
    Read = "READ",
    Write = "WRITE",
    Admin = "ADMIN",
    Select = "SELECT",
    Manage = "MANAGE",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}




    export function getJsonObj(obj: VolumePermissionSummary): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: VolumePermissionSummary): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
