// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Large Language Model configuration
*/
export interface LlmConfig {
    /**
    * The unique identifier of the Large Language Model (LLM) to use in the Agent or Tool
    */
    'modelId'?: string;
    /**
    * The Large language model provider name
    */
    'provider'?: string;
    /**
    * The Large language model Region ID
    */
    'regionId'?: string;

}

export namespace LlmConfig {




    export function getJsonObj(obj: LlmConfig): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LlmConfig): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
