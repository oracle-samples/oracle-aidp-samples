// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information to be updated for KnowledgeBase source.
*/
export interface UpdateKnowledgeBaseSourceUpdateDetailsItem {
    /**
    * Source update action type
    */
    'action': model.KnowledgeBaseSourceUpdateOperationType;
    'updateKnowledgeBaseAddSourceDetails'?: model.UpdateKnowledgeBaseAddSourceDetails;
    'updateKnowledgeBaseDeleteSourceDetails'?: model.UpdateKnowledgeBaseDeleteSourceDetails;

}

export namespace UpdateKnowledgeBaseSourceUpdateDetailsItem {




    export function getJsonObj(obj: UpdateKnowledgeBaseSourceUpdateDetailsItem): object {
        const jsonObj = {...obj, ...{
            

                'updateKnowledgeBaseAddSourceDetails': obj.updateKnowledgeBaseAddSourceDetails ?
                
                
                model.UpdateKnowledgeBaseAddSourceDetails.getJsonObj(obj.updateKnowledgeBaseAddSourceDetails) : undefined,
                'updateKnowledgeBaseDeleteSourceDetails': obj.updateKnowledgeBaseDeleteSourceDetails ?
                
                
                model.UpdateKnowledgeBaseDeleteSourceDetails.getJsonObj(obj.updateKnowledgeBaseDeleteSourceDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateKnowledgeBaseSourceUpdateDetailsItem): object {
        const jsonObj = {...obj, ...{
            

                    'updateKnowledgeBaseAddSourceDetails': obj.updateKnowledgeBaseAddSourceDetails ?
                
                
                model.UpdateKnowledgeBaseAddSourceDetails.getDeserializedJsonObj(obj.updateKnowledgeBaseAddSourceDetails) : undefined,
                    'updateKnowledgeBaseDeleteSourceDetails': obj.updateKnowledgeBaseDeleteSourceDetails ?
                
                
                model.UpdateKnowledgeBaseDeleteSourceDetails.getDeserializedJsonObj(obj.updateKnowledgeBaseDeleteSourceDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
