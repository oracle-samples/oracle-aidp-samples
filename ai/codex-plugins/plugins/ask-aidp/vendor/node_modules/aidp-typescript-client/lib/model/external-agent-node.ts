// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An External Agent Node in an Agent Flow.
*/
export interface ExternalAgentNode extends model.AgentFlowNode {
    /**
    * Custom prompt written by the user defining the agent\u2019s goal(s) and what tools the agent has access to
    */
    'instructions'?: string;
    /**
    * Extra configuration for the external agent node.
    */
    'externalAgentConfig'?: { [key: string]: string; };

   "type": string;
}

export namespace ExternalAgentNode {



    export function getJsonObj(obj: ExternalAgentNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as ExternalAgentNode, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const type = 'EXTERNAL_AGENT';
    export function getDeserializedJsonObj(obj: ExternalAgentNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as ExternalAgentNode, ...{
            


         }};

        
        
        return jsonObj;
    }
}
