// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of assignees for this role.
*/
export interface AddMemberToRoleDetails {
    /**
    * The assignees on the role.
    */
    'assignees': Array<model.RoleAssignee>;

}

export namespace AddMemberToRoleDetails {


    export function getJsonObj(obj: AddMemberToRoleDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                obj.assignees.map((item)=>{return model.RoleAssignee.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AddMemberToRoleDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                obj.assignees.map((item)=>{return model.RoleAssignee.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
