// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The input schema definition of a RAG Tool
*/
export interface RagToolInputSchema {
    /**
    * The user question to answer using relevant documents
    */
    'query'?: string;

}

export namespace RagToolInputSchema {


    export function getJsonObj(obj: RagToolInputSchema): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RagToolInputSchema): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
