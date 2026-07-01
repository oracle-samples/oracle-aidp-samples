// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tool configurations are set by the agent developer when they create the tool. | The agent does not see those configurations and can not modify their values
*/
export interface PromptToolConfiguration {
    'llm'?: model.LlmConfig;
    /**
    * The generative AI prompt
    */
    'promptText'?: string;
    /**
    * Model specific inference parameters such as temperature, top-k, max length, response format, etc.
    */
    'modelSettings'?: { [key: string]: any; };

}

export namespace PromptToolConfiguration {




    export function getJsonObj(obj: PromptToolConfiguration): object {
        const jsonObj = {...obj, ...{
            
                'llm': obj.llm ?
                
                
                model.LlmConfig.getJsonObj(obj.llm) : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PromptToolConfiguration): object {
        const jsonObj = {...obj, ...{
            
                    'llm': obj.llm ?
                
                
                model.LlmConfig.getDeserializedJsonObj(obj.llm) : undefined,


         }};

        
        
        return jsonObj;
    }
}
