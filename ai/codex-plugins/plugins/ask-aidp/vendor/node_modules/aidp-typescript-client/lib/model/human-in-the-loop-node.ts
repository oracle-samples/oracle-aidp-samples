// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A node used to represent a human interaction in the flow.
*/
export interface HumanInTheLoopNode extends model.AgentFlowNode {
    /**
    * Extra configuration for the human in the loop node.
    */
    'humanInTheLoopConfig'?: { [key: string]: string; };

   "type": string;
}

export namespace HumanInTheLoopNode {


    export function getJsonObj(obj: HumanInTheLoopNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as HumanInTheLoopNode, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'HUMAN_IN_THE_LOOP';
    export function getDeserializedJsonObj(obj: HumanInTheLoopNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as HumanInTheLoopNode, ...{
            

         }};

        
        
        return jsonObj;
    }
}
