// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of Create Workspace permissions.
*/
export interface CreateWorkspacePermissionCollection {
    /**
    * List of Create Workspace permissions.
    */
    'items': Array<model.CreateWorkspacePermissionSummary>;

}

export namespace CreateWorkspacePermissionCollection {


    export function getJsonObj(obj: CreateWorkspacePermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.CreateWorkspacePermissionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateWorkspacePermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.CreateWorkspacePermissionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
