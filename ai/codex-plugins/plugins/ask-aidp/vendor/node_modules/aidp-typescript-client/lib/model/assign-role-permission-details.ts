// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the role to assignee.
*/
export interface AssignRolePermissionDetails {
    /**
    * A list of permissions, resourceTypes, and resourceKeys.
    */
    'permissionWithResourceDetails': Array<model.PermissionWithResourceDetails>;

}

export namespace AssignRolePermissionDetails {


    export function getJsonObj(obj: AssignRolePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'permissionWithResourceDetails': obj.permissionWithResourceDetails ?
                
                obj.permissionWithResourceDetails.map((item)=>{return model.PermissionWithResourceDetails.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignRolePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'permissionWithResourceDetails': obj.permissionWithResourceDetails ?
                
                obj.permissionWithResourceDetails.map((item)=>{return model.PermissionWithResourceDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
