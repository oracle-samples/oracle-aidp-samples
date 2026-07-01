// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a cluster permission.
*/
export interface ClusterPermissionSummary {
    /**
    * OCID of user/group and name in case of role.
    */
    'grantee': string;
    /**
    * Simplified name of the grantee.
    */
    'granteeName'?: string;
    /**
    * Type of grantee.
    */
    'granteeType': model.GranteeType;
    /**
    * Selected permissions for a cluster.
    */
    'granteePermissions': Array<ClusterPermissionSummary.GranteePermissions>;

}

export namespace ClusterPermissionSummary {




    export enum GranteePermissions {
    
    Read = "READ",
    Use = "USE",
    Admin = "ADMIN",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: ClusterPermissionSummary): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterPermissionSummary): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
