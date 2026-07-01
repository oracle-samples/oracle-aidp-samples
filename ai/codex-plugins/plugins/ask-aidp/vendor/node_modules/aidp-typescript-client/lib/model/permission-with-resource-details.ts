// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A permission string with resource details.
*/
export interface PermissionWithResourceDetails {
    /**
    * Permission or privilege name.
    */
    'permissions': model.AllPrivilegeType;
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

export namespace PermissionWithResourceDetails {




    export function getJsonObj(obj: PermissionWithResourceDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PermissionWithResourceDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
