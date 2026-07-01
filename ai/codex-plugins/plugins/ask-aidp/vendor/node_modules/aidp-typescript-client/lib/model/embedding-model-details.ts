// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The embedding models which need to be enabled along with their storage location.
*/
export interface EmbeddingModelDetails extends model.ExecuteDatabaseUserWorkflowsDetails {
    /**
    * CSV list of models, e.g. \"<all_MiniLM_L12_v2>,<all_MiniLM_L64_v2>\"
    */
    'onnxModelFiles'?: string;
    /**
    * Directory for the models listed in onnxModelFiles.
    */
    'locationUri'?: string;

   "actionType": string;
}

export namespace EmbeddingModelDetails {



    export function getJsonObj(obj: EmbeddingModelDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getJsonObj(obj) as EmbeddingModelDetails, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const actionType = 'LOAD_EMBEDDING_MODELS';
    export function getDeserializedJsonObj(obj: EmbeddingModelDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getDeserializedJsonObj(obj) as EmbeddingModelDetails, ...{
            


         }};

        
        
        return jsonObj;
    }
}
