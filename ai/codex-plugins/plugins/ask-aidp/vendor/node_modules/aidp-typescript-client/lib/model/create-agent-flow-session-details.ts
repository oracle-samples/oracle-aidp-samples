// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to create a new session of an Agent Flow.
*/
export interface CreateAgentFlowSessionDetails {
    /**
    * Display name.
    */
    'displayName'?: string;
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'agentFlowKey': string;
    'context'?: model.AgentFlowSessionContext;

}

export namespace CreateAgentFlowSessionDetails {




    export function getJsonObj(obj: CreateAgentFlowSessionDetails): object {
        const jsonObj = {...obj, ...{
            


                'context': obj.context ?
                
                
                model.AgentFlowSessionContext.getJsonObj(obj.context) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateAgentFlowSessionDetails): object {
        const jsonObj = {...obj, ...{
            


                    'context': obj.context ?
                
                
                model.AgentFlowSessionContext.getDeserializedJsonObj(obj.context) : undefined,
         }};

        
        
        return jsonObj;
    }
}
