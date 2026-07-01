// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a role.
*/
export interface RevokeRolePermissionDetails {
    /**
    * A list of permissions, resourceTypes, and resourceKeys.
    */
    'permissionWithResourceDetails': Array<model.PermissionWithResourceDetails>;

}

export namespace RevokeRolePermissionDetails {


    export function getJsonObj(obj: RevokeRolePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'permissionWithResourceDetails': obj.permissionWithResourceDetails ?
                
                obj.permissionWithResourceDetails.map((item)=>{return model.PermissionWithResourceDetails.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeRolePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'permissionWithResourceDetails': obj.permissionWithResourceDetails ?
                
                obj.permissionWithResourceDetails.map((item)=>{return model.PermissionWithResourceDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
