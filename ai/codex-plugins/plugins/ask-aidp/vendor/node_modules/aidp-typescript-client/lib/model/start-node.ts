// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A starting point in an agent flow.
*/
export interface StartNode extends model.AgentFlowNode {
    /**
    * Extra configuration for the start node.
    */
    'startNodeConfig'?: { [key: string]: string; };

   "type": string;
}

export namespace StartNode {


    export function getJsonObj(obj: StartNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as StartNode, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'START_NODE';
    export function getDeserializedJsonObj(obj: StartNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as StartNode, ...{
            

         }};

        
        
        return jsonObj;
    }
}
