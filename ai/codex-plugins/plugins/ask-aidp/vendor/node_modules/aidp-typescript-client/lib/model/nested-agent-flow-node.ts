// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An agent node representing a nested Agent Flow
*/
export interface NestedAgentFlowNode extends model.AgentFlowNode {
    /**
    * System prompt written by the flow developer defining the agent\u2019s goal(s) and what tools the agent has access to.
    */
    'instructions'?: string;
    'memory'?: model.MemoryConfiguration;
    /**
    * Extra configuration for the nested agent flow node.
    */
    'nestedAgentFlowConfig'?: { [key: string]: string; };

   "type": string;
}

export namespace NestedAgentFlowNode {




    export function getJsonObj(obj: NestedAgentFlowNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as NestedAgentFlowNode, ...{
            

                'memory': obj.memory ?
                
                
                model.MemoryConfiguration.getJsonObj(obj.memory) : undefined,

        }};

        
        
        return jsonObj;
    }
    export const type = 'NESTED_AGENT_FLOW';
    export function getDeserializedJsonObj(obj: NestedAgentFlowNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as NestedAgentFlowNode, ...{
            

                    'memory': obj.memory ?
                
                
                model.MemoryConfiguration.getDeserializedJsonObj(obj.memory) : undefined,

         }};

        
        
        return jsonObj;
    }
}
