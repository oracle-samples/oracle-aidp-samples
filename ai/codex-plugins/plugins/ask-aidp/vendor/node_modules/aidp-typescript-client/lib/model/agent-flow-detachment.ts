// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Detachment info of an Agent Flow.
*/
export interface AgentFlowDetachment {
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'agentFlowComputeKey': string;
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'agentFlowKey': string;

}

export namespace AgentFlowDetachment {



    export function getJsonObj(obj: AgentFlowDetachment): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowDetachment): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
