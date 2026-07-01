// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request details when toolType = RAG.
*/
export interface TestRagToolDetails extends model.TestToolDetails {
    'config': model.RagToolConfiguration;
    /**
    * The user query or instruction to be augmented with retrieved context.
    */
    'query': string;

   "toolType": string;
}

export namespace TestRagToolDetails {



    export function getJsonObj(obj: TestRagToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getJsonObj(obj) as TestRagToolDetails, ...{
            
                'config': obj.config ?
                
                
                model.RagToolConfiguration.getJsonObj(obj.config) : undefined,

        }};

        
        
        return jsonObj;
    }
    export const toolType = 'RAG';
    export function getDeserializedJsonObj(obj: TestRagToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestToolDetails.getDeserializedJsonObj(obj) as TestRagToolDetails, ...{
            
                    'config': obj.config ?
                
                
                model.RagToolConfiguration.getDeserializedJsonObj(obj.config) : undefined,

         }};

        
        
        return jsonObj;
    }
}
