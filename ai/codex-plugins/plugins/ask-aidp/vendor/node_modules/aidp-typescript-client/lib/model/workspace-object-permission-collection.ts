// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of workspace object permissions.
*/
export interface WorkspaceObjectPermissionCollection {
    /**
    * List of workspace object permissions.
    */
    'items': Array<model.WorkspaceObjectPermissionSummary>;

}

export namespace WorkspaceObjectPermissionCollection {


    export function getJsonObj(obj: WorkspaceObjectPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.WorkspaceObjectPermissionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceObjectPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.WorkspaceObjectPermissionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
