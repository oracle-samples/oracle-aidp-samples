// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Agent flow compute cluster details for creation.
*/
export interface CreateAgentFlowComputeDetails extends model.CreateClusterDetails {

   "type": string;
}

export namespace CreateAgentFlowComputeDetails {

    export function getJsonObj(obj: CreateAgentFlowComputeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateClusterDetails.getJsonObj(obj) as CreateAgentFlowComputeDetails, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const type = 'AGENT_FLOW_COMPUTE';
    export function getDeserializedJsonObj(obj: CreateAgentFlowComputeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateClusterDetails.getDeserializedJsonObj(obj) as CreateAgentFlowComputeDetails, ...{
            
         }};

        
        
        return jsonObj;
    }
}
