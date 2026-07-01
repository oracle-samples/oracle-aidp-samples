// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to update a new session of an Agent Flow.
*/
export interface UpdateAgentFlowSessionDetails {
    /**
    * Display name.
    */
    'displayName'?: string;
    /**
    * The date and time the session was started
    */
    'timeStarted'?: Date;
    /**
    * The date and time the session was ended
    */
    'timeEnded'?: Date;
    'context'?: model.AgentFlowSessionContext;

}

export namespace UpdateAgentFlowSessionDetails {





    export function getJsonObj(obj: UpdateAgentFlowSessionDetails): object {
        const jsonObj = {...obj, ...{
            



                'context': obj.context ?
                
                
                model.AgentFlowSessionContext.getJsonObj(obj.context) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateAgentFlowSessionDetails): object {
        const jsonObj = {...obj, ...{
            



                    'context': obj.context ?
                
                
                model.AgentFlowSessionContext.getDeserializedJsonObj(obj.context) : undefined,
         }};

        
        
        return jsonObj;
    }
}
