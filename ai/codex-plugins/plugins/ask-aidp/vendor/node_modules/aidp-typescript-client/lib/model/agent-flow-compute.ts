// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* User-created cluster.
*/
export interface AgentFlowCompute extends model.Cluster {

   "sourceApi": string;
}

export namespace AgentFlowCompute {

    export function getJsonObj(obj: AgentFlowCompute, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Cluster.getJsonObj(obj) as AgentFlowCompute, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const sourceApi = 'AGENT_FLOW_COMPUTE';
    export function getDeserializedJsonObj(obj: AgentFlowCompute, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Cluster.getDeserializedJsonObj(obj) as AgentFlowCompute, ...{
            
         }};

        
        
        return jsonObj;
    }
}
