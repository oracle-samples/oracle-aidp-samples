// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tool configurations are set by the agent developer when they create the tool. | The agent does not see those configurations and can NOT modify their values
*/
export interface RagToolConfiguration {
    /**
    * The Catalog to use for KB tool execution
    */
    'catalogKey'?: string;
    /**
    * The Schema to use for KB tool execution
    */
    'schemaKey'?: string;
    /**
    * The name of the Knowledge Base to use for RAG query
    */
    'knowledgeBase'?: string;
    'llm'?: model.LlmConfig;
    /**
    * Model specific inference parameters such as temperature, top-k, max length, response format, etc.
    */
    'modelSettings'?: { [key: string]: any; };
    /**
    * Number of top chunks to retrieve from the KB Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'topK'?: number;

}

export namespace RagToolConfiguration {







    export function getJsonObj(obj: RagToolConfiguration): object {
        const jsonObj = {...obj, ...{
            



                'llm': obj.llm ?
                
                
                model.LlmConfig.getJsonObj(obj.llm) : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RagToolConfiguration): object {
        const jsonObj = {...obj, ...{
            



                    'llm': obj.llm ?
                
                
                model.LlmConfig.getDeserializedJsonObj(obj.llm) : undefined,


         }};

        
        
        return jsonObj;
    }
}
