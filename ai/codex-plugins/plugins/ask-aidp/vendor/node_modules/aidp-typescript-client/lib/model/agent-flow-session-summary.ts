// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Agent flow session summary.
*/
export interface AgentFlowSessionSummary {
    /**
    * Display name.
    */
    'displayName': string;
    /**
    * Agent Flow Session identifier.
    */
    'key': string;
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'agentFlowKey': string;
    /**
    * LifecycleState of an Agent Flow Session or Deployment.
    */
    'lifecycleState'?: model.DeploymentLifecycleState;
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'agentFlowComputeKey'?: string;
    /**
    * The endpointUrl where the client should connect to communicate with the Agent.
    */
    'endpointUrl'?: string;
    /**
    * The date and time the Agent flow session was created.
    */
    'timeCreated': Date;
    /**
    * The OCID of the user/principal who created the Agent flow session.
    */
    'createdBy': string;
    /**
    * The date and time the session was started
    */
    'timeStarted'?: Date;
    /**
    * The date and time the session was ended
    */
    'timeEnded'?: Date;
    /**
    * Agent flow session duration Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'duration'?: number;
    /**
    * Agent flow session token usage Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'tokens'?: number;

}

export namespace AgentFlowSessionSummary {













    export function getJsonObj(obj: AgentFlowSessionSummary): object {
        const jsonObj = {...obj, ...{
            












        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowSessionSummary): object {
        const jsonObj = {...obj, ...{
            












         }};

        
        
        return jsonObj;
    }
}
