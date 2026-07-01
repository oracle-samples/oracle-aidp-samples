// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update an Agent Node in an Agent Flow
*/
export interface UpdateAgentNodeDetails extends model.UpdateAgentFlowNodeDetails {
    /**
    * System prompt written by the flow developer defining the agent\u2019s goal(s) and what tools the agent has access to.
    */
    'instructions'?: string;
    'llm'?: model.LlmConfig;
    /**
    * Model specific inference parameters such as temperature, top-k, max length, response format, etc.
    */
    'modelSettings'?: { [key: string]: any; };
    'memory'?: model.MemoryConfiguration;
    /**
    * List of tools that are accessible to the agent. Provide the unique tool key
    */
    'tools'?: Array<model.Tool>;

   "type": string;
}

export namespace UpdateAgentNodeDetails {






    export function getJsonObj(obj: UpdateAgentNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getJsonObj(obj) as UpdateAgentNodeDetails, ...{
            

                'llm': obj.llm ?
                
                
                model.LlmConfig.getJsonObj(obj.llm) : undefined,

                'memory': obj.memory ?
                
                
                model.MemoryConfiguration.getJsonObj(obj.memory) : undefined,
                'tools': obj.tools ?
                
                obj.tools.map((item)=>{return model.Tool.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'AGENT';
    export function getDeserializedJsonObj(obj: UpdateAgentNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as UpdateAgentNodeDetails, ...{
            

                    'llm': obj.llm ?
                
                
                model.LlmConfig.getDeserializedJsonObj(obj.llm) : undefined,

                    'memory': obj.memory ?
                
                
                model.MemoryConfiguration.getDeserializedJsonObj(obj.memory) : undefined,
                    'tools': obj.tools ?
                
                obj.tools.map((item)=>{return model.Tool.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
