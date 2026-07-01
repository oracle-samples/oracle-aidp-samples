// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single retrieved document chunk with metadata.
*/
export interface RetrievedChunk {
    /**
    * Unique identifier or URI for the document chunk.
    */
    'documentId'?: string;
    /**
    * Text content of the retrieved chunk.
    */
    'content'?: string;
    /**
    * Relevance score assigned to the chunk during retrieval. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'score'?: number;

}

export namespace RetrievedChunk {




    export function getJsonObj(obj: RetrievedChunk): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RetrievedChunk): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
