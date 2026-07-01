// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Structured RAG result containing generated answer and retrieved document chunks.
*/
export interface RagResult {
    /**
    * The synthesized response generated using retrieved documents.
    */
    'answer': string;
    /**
    * List of document chunks retrieved during RAG processing.
    */
    'retrievedChunks': Array<model.RetrievedChunk>;

}

export namespace RagResult {



    export function getJsonObj(obj: RagResult): object {
        const jsonObj = {...obj, ...{
            

                'retrievedChunks': obj.retrievedChunks ?
                
                obj.retrievedChunks.map((item)=>{return model.RetrievedChunk.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RagResult): object {
        const jsonObj = {...obj, ...{
            

                    'retrievedChunks': obj.retrievedChunks ?
                
                obj.retrievedChunks.map((item)=>{return model.RetrievedChunk.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
