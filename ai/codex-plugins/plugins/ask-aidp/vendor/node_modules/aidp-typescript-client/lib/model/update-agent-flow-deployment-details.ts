// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to update the deployment of an Agent Flow.
*/
export interface UpdateAgentFlowDeploymentDetails {
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
    'oAuthConfig'?: model.OAuthConfiguration;

}

export namespace UpdateAgentFlowDeploymentDetails {







    export function getJsonObj(obj: UpdateAgentFlowDeploymentDetails): object {
        const jsonObj = {...obj, ...{
            





                'oAuthConfig': obj.oAuthConfig ?
                
                
                model.OAuthConfiguration.getJsonObj(obj.oAuthConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateAgentFlowDeploymentDetails): object {
        const jsonObj = {...obj, ...{
            





                    'oAuthConfig': obj.oAuthConfig ?
                
                
                model.OAuthConfiguration.getDeserializedJsonObj(obj.oAuthConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
