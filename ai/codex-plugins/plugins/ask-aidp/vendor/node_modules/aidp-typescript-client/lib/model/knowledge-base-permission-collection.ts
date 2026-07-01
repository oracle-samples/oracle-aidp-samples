// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of Knowledge Base permissions.
*/
export interface KnowledgeBasePermissionCollection {
    /**
    * List of Knowledge Base permissions.
    */
    'items': Array<model.KnowledgeBasePermissionSummary>;

}

export namespace KnowledgeBasePermissionCollection {


    export function getJsonObj(obj: KnowledgeBasePermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBasePermissionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBasePermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBasePermissionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
