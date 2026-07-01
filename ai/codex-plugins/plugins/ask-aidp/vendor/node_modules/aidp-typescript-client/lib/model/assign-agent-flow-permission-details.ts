// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the Agent flow to assignee.
*/
export interface AssignAgentFlowPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees
    */
    'permissions': Array<AssignAgentFlowPermissionDetails.Permissions>;
    /**
    * The list of columns to be included for the assigning of permissions
    */
    'includeColumns': Array<string>;
    /**
    * The list of columns to be excluded for the assigning of permissions
    */
    'excludeColumns': Array<string>;

}

export namespace AssignAgentFlowPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"

}




    export function getJsonObj(obj: AssignAgentFlowPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignAgentFlowPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,



         }};

        
        
        return jsonObj;
    }
}
