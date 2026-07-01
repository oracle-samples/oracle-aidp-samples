// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a Knowledge Base.
*/
export interface ManageKnowledgeBasePermissionDetails {
    'assignKnowledgeBasePermissionDetails'?: model.AssignKnowledgeBasePermissionDetails;
    'revokeKnowledgeBasePermissionDetails'?: model.RevokeKnowledgeBasePermissionDetails;

}

export namespace ManageKnowledgeBasePermissionDetails {



    export function getJsonObj(obj: ManageKnowledgeBasePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignKnowledgeBasePermissionDetails': obj.assignKnowledgeBasePermissionDetails ?
                
                
                model.AssignKnowledgeBasePermissionDetails.getJsonObj(obj.assignKnowledgeBasePermissionDetails) : undefined,
                'revokeKnowledgeBasePermissionDetails': obj.revokeKnowledgeBasePermissionDetails ?
                
                
                model.RevokeKnowledgeBasePermissionDetails.getJsonObj(obj.revokeKnowledgeBasePermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageKnowledgeBasePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignKnowledgeBasePermissionDetails': obj.assignKnowledgeBasePermissionDetails ?
                
                
                model.AssignKnowledgeBasePermissionDetails.getDeserializedJsonObj(obj.assignKnowledgeBasePermissionDetails) : undefined,
                    'revokeKnowledgeBasePermissionDetails': obj.revokeKnowledgeBasePermissionDetails ?
                
                
                model.RevokeKnowledgeBasePermissionDetails.getDeserializedJsonObj(obj.revokeKnowledgeBasePermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
