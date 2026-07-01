// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about new KnowledgeBase.
*/
export interface CreateKnowledgeBaseDetails {
    /**
    * Name of KnowledgeBase
    */
    'displayName': string;
    /**
    * The description of KnowledgeBase.
    */
    'description'?: string;
    /**
    * The key of the catalog containing the KnowledgeBase.
    */
    'catalogKey': string;
    /**
    * type of KnowledgeBase
    */
    'type': model.KnowledgeBaseType;
    /**
    * The key of the schema containing the Knowledgebase.
    */
    'schemaKey': string;
    /**
    * The id of the workspace associated with the KnowledgeBase.
    */
    'workspaceKey': string;
    /**
    * The id of the cluster associated with the KnowledgeBase.
    */
    'clusterKey': string;
    /**
    * Modality of the data in this KnowledgeBase
    */
    'modality'?: model.KnowledgeBaseModality;
    /**
    * Information about where embedding model is located
    */
    'embeddingModelSourceType'?: model.KnowledgeBaseEmbeddingModelSourceType;
    /**
    * Name of the embedding model
    */
    'embeddingModelName'?: string;
    /**
    * Chunk size at KnowledgeBase level which can be overridden by source level settings Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'chunkSize'?: number;
    /**
    * Chunk Overlap at KnowledgeBase level which can be overridden by source level settings Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'chunkOverlap'?: number;
    /**
    * Applicable for Native KnowledgeBase where source type is KnowledgeBase
    */
    'sourceFilePattern'?: string;
    'indexDetails'?: model.KnowledgeBaseVectorIndexDetails;

}

export namespace CreateKnowledgeBaseDetails {















    export function getJsonObj(obj: CreateKnowledgeBaseDetails): object {
        const jsonObj = {...obj, ...{
            













                'indexDetails': obj.indexDetails ?
                
                
                model.KnowledgeBaseVectorIndexDetails.getJsonObj(obj.indexDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateKnowledgeBaseDetails): object {
        const jsonObj = {...obj, ...{
            













                    'indexDetails': obj.indexDetails ?
                
                
                model.KnowledgeBaseVectorIndexDetails.getDeserializedJsonObj(obj.indexDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
