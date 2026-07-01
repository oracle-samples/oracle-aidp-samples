// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Agentflow deployment details.
*/
export interface AgentFlowDeployment {
    /**
    * Identifier, generally uuid.
    */
    'key': string;
    /**
    * Display name.
    */
    'displayName': string;
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'agentFlowKey': string;
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'agentFlowComputeKey'?: string;
    /**
    * The endpointUrl where the client should connect to communicate with the Agent.
    */
    'endpointUrl': string;
    /**
    * deployment description.
    */
    'description'?: string;
    /**
    * Type of an Agent Flow Deployment.
    */
    'deploymentType': model.DeploymentType;
    /**
    * LifecycleState of an Agent Flow Session or Deployment.
    */
    'lifecycleState': model.DeploymentLifecycleState;
    /**
    * version of agent flow deployed on compute.
    */
    'deploymentVersion': string;
    /**
    * The date and time the Agent flow session was created.
    */
    'timeCreated': Date;
    /**
    * The OCID of the user/principal who created the Agent flow session.
    */
    'createdBy': string;
    /**
    * The date and time the Agent flow deployment was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The OCID of the user/principal who re-deployed the existing Agent flow deployment.
    */
    'updatedBy'?: string;
    'sessionRetentionConfig'?: model.SessionRetentionConfiguration;
    'oAuthConfig'?: model.OAuthConfiguration;
    /**
    * AgentCard base URL
    */
    'agentCardUrl'?: string;

}

export namespace AgentFlowDeployment {

















    export function getJsonObj(obj: AgentFlowDeployment): object {
        const jsonObj = {...obj, ...{
            













                'sessionRetentionConfig': obj.sessionRetentionConfig ?
                
                
                model.SessionRetentionConfiguration.getJsonObj(obj.sessionRetentionConfig) : undefined,
                'oAuthConfig': obj.oAuthConfig ?
                
                
                model.OAuthConfiguration.getJsonObj(obj.oAuthConfig) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowDeployment): object {
        const jsonObj = {...obj, ...{
            













                    'sessionRetentionConfig': obj.sessionRetentionConfig ?
                
                
                model.SessionRetentionConfiguration.getDeserializedJsonObj(obj.sessionRetentionConfig) : undefined,
                    'oAuthConfig': obj.oAuthConfig ?
                
                
                model.OAuthConfiguration.getDeserializedJsonObj(obj.oAuthConfig) : undefined,

         }};

        
        
        return jsonObj;
    }
}
