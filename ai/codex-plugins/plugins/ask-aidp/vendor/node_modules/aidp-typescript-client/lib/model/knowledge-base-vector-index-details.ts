// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Index configuration details
*/
export interface KnowledgeBaseVectorIndexDetails {
    /**
    * Type of vector index (HNSW or IVF)
    */
    'type'?: model.KnowledgeBaseVectorIndexType;
    'hnswDetails'?: model.KbVHnswIndexDetails;
    'ivfDetails'?: model.KbVIvfIndexDetails;

}

export namespace KnowledgeBaseVectorIndexDetails {




    export function getJsonObj(obj: KnowledgeBaseVectorIndexDetails): object {
        const jsonObj = {...obj, ...{
            

                'hnswDetails': obj.hnswDetails ?
                
                
                model.KbVHnswIndexDetails.getJsonObj(obj.hnswDetails) : undefined,
                'ivfDetails': obj.ivfDetails ?
                
                
                model.KbVIvfIndexDetails.getJsonObj(obj.ivfDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseVectorIndexDetails): object {
        const jsonObj = {...obj, ...{
            

                    'hnswDetails': obj.hnswDetails ?
                
                
                model.KbVHnswIndexDetails.getDeserializedJsonObj(obj.hnswDetails) : undefined,
                    'ivfDetails': obj.ivfDetails ?
                
                
                model.KbVIvfIndexDetails.getDeserializedJsonObj(obj.ivfDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
