// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Agent Flow Compute cluster details for creation
*/
export interface UpdateAgentFlowComputeDetails extends model.UpdateClusterDetails {

   "type": string;
}

export namespace UpdateAgentFlowComputeDetails {

    export function getJsonObj(obj: UpdateAgentFlowComputeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateClusterDetails.getJsonObj(obj) as UpdateAgentFlowComputeDetails, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const type = 'AGENT_FLOW_COMPUTE';
    export function getDeserializedJsonObj(obj: UpdateAgentFlowComputeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateClusterDetails.getDeserializedJsonObj(obj) as UpdateAgentFlowComputeDetails, ...{
            
         }};

        
        
        return jsonObj;
    }
}
