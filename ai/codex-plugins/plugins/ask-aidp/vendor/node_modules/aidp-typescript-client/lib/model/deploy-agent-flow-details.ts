// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Agentflow details to deploy an agentflow.
*/
export interface DeployAgentFlowDetails {
    /**
    * Display name.
    */
    'displayName'?: string;
    /**
    * deployment description.
    */
    'description'?: string;
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'agentFlowComputeKey'?: string;
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'agentFlowKey': string;
    /**
    * Type of an Agent Flow Deployment.
    */
    'deploymentType': model.DeploymentType;
    'sessionRetentionConfig'?: model.SessionRetentionConfiguration;
    'oAuthConfig'?: model.OAuthConfiguration;

}

export namespace DeployAgentFlowDetails {








    export function getJsonObj(obj: DeployAgentFlowDetails): object {
        const jsonObj = {...obj, ...{
            





                'sessionRetentionConfig': obj.sessionRetentionConfig ?
                
                
                model.SessionRetentionConfiguration.getJsonObj(obj.sessionRetentionConfig) : undefined,
                'oAuthConfig': obj.oAuthConfig ?
                
                
                model.OAuthConfiguration.getJsonObj(obj.oAuthConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeployAgentFlowDetails): object {
        const jsonObj = {...obj, ...{
            





                    'sessionRetentionConfig': obj.sessionRetentionConfig ?
                
                
                model.SessionRetentionConfiguration.getDeserializedJsonObj(obj.sessionRetentionConfig) : undefined,
                    'oAuthConfig': obj.oAuthConfig ?
                
                
                model.OAuthConfiguration.getDeserializedJsonObj(obj.oAuthConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
