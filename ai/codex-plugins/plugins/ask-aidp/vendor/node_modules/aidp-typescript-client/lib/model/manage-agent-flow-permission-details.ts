// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a Agent flow.
*/
export interface ManageAgentFlowPermissionDetails {
    'assignAgentFlowPermissionDetails'?: model.AssignAgentFlowPermissionDetails;
    'revokeAgentFlowPermissionDetails'?: model.RevokeAgentFlowPermissionDetails;

}

export namespace ManageAgentFlowPermissionDetails {



    export function getJsonObj(obj: ManageAgentFlowPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignAgentFlowPermissionDetails': obj.assignAgentFlowPermissionDetails ?
                
                
                model.AssignAgentFlowPermissionDetails.getJsonObj(obj.assignAgentFlowPermissionDetails) : undefined,
                'revokeAgentFlowPermissionDetails': obj.revokeAgentFlowPermissionDetails ?
                
                
                model.RevokeAgentFlowPermissionDetails.getJsonObj(obj.revokeAgentFlowPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageAgentFlowPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignAgentFlowPermissionDetails': obj.assignAgentFlowPermissionDetails ?
                
                
                model.AssignAgentFlowPermissionDetails.getDeserializedJsonObj(obj.assignAgentFlowPermissionDetails) : undefined,
                    'revokeAgentFlowPermissionDetails': obj.revokeAgentFlowPermissionDetails ?
                
                
                model.RevokeAgentFlowPermissionDetails.getDeserializedJsonObj(obj.revokeAgentFlowPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
