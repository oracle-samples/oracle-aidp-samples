// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Agent flow session.
*/
export interface AgentFlowSession {
    /**
    * Display name.
    */
    'displayName': string;
    /**
    * Agent Flow Session identifier.
    */
    'key': string;
    /**
    * The Agent Flow key for which the session is started.
    */
    'agentFlowKey': string;
    /**
    * The endpointUrl where the client should connect to communicate with the Agent.
    */
    'endpointUrl': string;
    /**
    * LifecycleState of an Agent Flow Session or Deployment.
    */
    'lifecycleState': model.DeploymentLifecycleState;
    /**
    * The Agent Flow Compute Key where client can run or test the Agent Flow.
    */
    'agentFlowComputeKey': string;
    /**
    * The date and time the Agent flow session was created.
    */
    'timeCreated'?: Date;
    /**
    * OCID of the user who updated this record
    */
    'updatedBy'?: string;
    /**
    * The OCID of the user/principal who created the Agent flow session.
    */
    'createdBy'?: string;
    /**
    * The date and time the Agent flow session was updated.
    */
    'timeUpdated'?: Date;
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

export namespace AgentFlowSession {














    export function getJsonObj(obj: AgentFlowSession): object {
        const jsonObj = {...obj, ...{
            












                'context': obj.context ?
                
                
                model.AgentFlowSessionContext.getJsonObj(obj.context) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowSession): object {
        const jsonObj = {...obj, ...{
            












                    'context': obj.context ?
                
                
                model.AgentFlowSessionContext.getDeserializedJsonObj(obj.context) : undefined,
         }};

        
        
        return jsonObj;
    }
}
