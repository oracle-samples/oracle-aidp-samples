// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of permissions/privileges with resource details.
*/
export interface ListPermissionsWithResourceDetails {
    /**
    * List of privilege names.
    */
    'permissions': Array<model.AllPrivilegeType>;
    /**
    * All sub-resources in catalog and workspace.
    */
    'resourceType': model.AllResourceType;
    /**
    * Workspace and its sub-resources key.
* For example - For workspaceKey, clusterKey its a UUID
* Within catalog its a 3 level namespace
*    tableKey - <catalogName>.<schemaName>.<tableName> 
*    schemaKey - <catalogName>.<schemaName>
*    catalogKey - <catalogName>
* 
    */
    'resourceKey': string;

}

export namespace ListPermissionsWithResourceDetails {




    export function getJsonObj(obj: ListPermissionsWithResourceDetails): object {
        const jsonObj = {...obj, ...{
            
                'permissions': obj.permissions ?
                
                obj.permissions.map((item)=>{return model.AllPrivilegeType.getJsonObj(item)})
                
                 : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ListPermissionsWithResourceDetails): object {
        const jsonObj = {...obj, ...{
            
                    'permissions': obj.permissions ?
                
                obj.permissions.map((item)=>{return model.AllPrivilegeType.getDeserializedJsonObj(item)})
                
                 : undefined,


         }};

        
        
        return jsonObj;
    }
}
