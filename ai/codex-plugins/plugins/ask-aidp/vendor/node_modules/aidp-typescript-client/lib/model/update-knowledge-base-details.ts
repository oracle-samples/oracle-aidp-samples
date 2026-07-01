// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information to be updated.
*/
export interface UpdateKnowledgeBaseDetails {
    /**
    * The update operation we want to perform on KnowledgeBase.
    */
    'action': model.KnowledgeBaseUpdateOperationType;
    'indexDetails'?: model.KnowledgeBaseVectorIndexDetails;
    'updateKnowledgeBaseMetadataUpdateDetails'?: model.UpdateKnowledgeBaseMetadataUpdateDetails;
    'updateKnowledgeBaseSourceUpdateDetails'?: model.UpdateKnowledgeBaseSourceUpdateDetails;

}

export namespace UpdateKnowledgeBaseDetails {





    export function getJsonObj(obj: UpdateKnowledgeBaseDetails): object {
        const jsonObj = {...obj, ...{
            

                'indexDetails': obj.indexDetails ?
                
                
                model.KnowledgeBaseVectorIndexDetails.getJsonObj(obj.indexDetails) : undefined,
                'updateKnowledgeBaseMetadataUpdateDetails': obj.updateKnowledgeBaseMetadataUpdateDetails ?
                
                
                model.UpdateKnowledgeBaseMetadataUpdateDetails.getJsonObj(obj.updateKnowledgeBaseMetadataUpdateDetails) : undefined,
                'updateKnowledgeBaseSourceUpdateDetails': obj.updateKnowledgeBaseSourceUpdateDetails ?
                
                
                model.UpdateKnowledgeBaseSourceUpdateDetails.getJsonObj(obj.updateKnowledgeBaseSourceUpdateDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateKnowledgeBaseDetails): object {
        const jsonObj = {...obj, ...{
            

                    'indexDetails': obj.indexDetails ?
                
                
                model.KnowledgeBaseVectorIndexDetails.getDeserializedJsonObj(obj.indexDetails) : undefined,
                    'updateKnowledgeBaseMetadataUpdateDetails': obj.updateKnowledgeBaseMetadataUpdateDetails ?
                
                
                model.UpdateKnowledgeBaseMetadataUpdateDetails.getDeserializedJsonObj(obj.updateKnowledgeBaseMetadataUpdateDetails) : undefined,
                    'updateKnowledgeBaseSourceUpdateDetails': obj.updateKnowledgeBaseSourceUpdateDetails ?
                
                
                model.UpdateKnowledgeBaseSourceUpdateDetails.getDeserializedJsonObj(obj.updateKnowledgeBaseSourceUpdateDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
