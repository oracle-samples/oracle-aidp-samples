// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Attachment info of an Agent Flow.
*/
export interface AgentFlowAttachment {
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'agentFlowComputeKey': string;
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'agentFlowKey': string;

}

export namespace AgentFlowAttachment {



    export function getJsonObj(obj: AgentFlowAttachment): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowAttachment): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
