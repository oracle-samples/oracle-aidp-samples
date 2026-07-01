// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary of role permissions.
*/
export interface RolePermissionSummary {
    'permissionsWithResourceDetails': model.ListPermissionsWithResourceDetails;
    /**
    * Role attached to this permission entry.
    */
    'roleKey': string;
    /**
    * The description of the role.
    */
    'roleDescription'?: string;

}

export namespace RolePermissionSummary {




    export function getJsonObj(obj: RolePermissionSummary): object {
        const jsonObj = {...obj, ...{
            
                'permissionsWithResourceDetails': obj.permissionsWithResourceDetails ?
                
                
                model.ListPermissionsWithResourceDetails.getJsonObj(obj.permissionsWithResourceDetails) : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RolePermissionSummary): object {
        const jsonObj = {...obj, ...{
            
                    'permissionsWithResourceDetails': obj.permissionsWithResourceDetails ?
                
                
                model.ListPermissionsWithResourceDetails.getDeserializedJsonObj(obj.permissionsWithResourceDetails) : undefined,


         }};

        
        
        return jsonObj;
    }
}
