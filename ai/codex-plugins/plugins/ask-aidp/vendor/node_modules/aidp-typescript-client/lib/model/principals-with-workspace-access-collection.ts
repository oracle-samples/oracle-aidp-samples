// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Returns a list of users with particular AI Data Platform Workbench RBAC permissions across workspaces.
*/
export interface PrincipalsWithWorkspaceAccessCollection {
    /**
    * List of users with particular AI Data Platform Workbench RBAC permissions across workspaces.
    */
    'items': Array<model.PrincipalsWithWorkspaceAccessSummary>;

}

export namespace PrincipalsWithWorkspaceAccessCollection {


    export function getJsonObj(obj: PrincipalsWithWorkspaceAccessCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.PrincipalsWithWorkspaceAccessSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PrincipalsWithWorkspaceAccessCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.PrincipalsWithWorkspaceAccessSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
