// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An agent flow is a sequence of nodes and edges defining an end-to-end agentic application. | A flow can be composed of multiple nodes, each node being a single agent or a multi-agent system, working independently | or collaboratively to accomplish an overall objective
*/
export interface AgentFlow {
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'key': string;
    /**
    * AgentFlow name.
    */
    'displayName': string;
    /**
    * The key of the workspace to which this Agent flow belongs.
    */
    'workspaceKey'?: string;
    /**
    * AgentFlow description.
    */
    'description': string;
    /**
    * Path inside volume where the agentFlow json is written
    */
    'pathInfo': string;
    /**
    * The type of Agent Flow (Canvas or Code)
    */
    'type'?: AgentFlow.Type;
    /**
    * The path to project entry file
    */
    'entryFilePath'?: string;
    /**
    * The path to dependencies file
    */
    'dependenciesFilePath'?: string;
    /**
    * The key of the Compute where Agent Flow is deployed
    */
    'deploymentComputeKey'?: string;
    /**
    * Agent flow deployment mode.
    */
    'deploymentMode'?: string;
    /**
    * Agent flow uri.
    */
    'uri'?: string;
    /**
    * Agent flow uri state.
    */
    'uriState'?: string;
    /**
    * The current state of the Agent Flow.
    */
    'lifecycleState': AgentFlow.LifecycleState;
    /**
    * A message that describes the current state of the Agent Flow in more detail. For example,
* can be used to provide actionable information for a resource in the Failed state.
* 
    */
    'lifecycleDetails'?: string;
    /**
    * The date and time the Agent flow was created.
    */
    'timeCreated'?: Date;
    /**
    * The date and time the Agent flow was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The OCID of the user/principal who created the Agent flow.
    */
    'createdBy'?: string;
    /**
    * The ID of the user who last updated the schema.
    */
    'updatedBy'?: string;
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'computeKey'?: string;
    'diagram'?: model.AgentFlowDiagram;
    'guardrails'?: model.GuardrailsConfiguration;
    'sessionConfig'?: model.SessionConfiguration;
    'agentCardConfig'?: model.AgentCardConfigDetail;
    /**
    * A number indicating the version of the record. Each time the record is updated, this version will be incremented. This will be used for optimistic locking Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'version'?: number;

}

export namespace AgentFlow {






    export enum Type {
    
    Canvas = "CANVAS",
    Code = "CODE"

}








    export enum LifecycleState {
    
    Draft = "DRAFT",
    Deployed = "DEPLOYED"

}













    export function getJsonObj(obj: AgentFlow): object {
        const jsonObj = {...obj, ...{
            



















                'diagram': obj.diagram ?
                
                
                model.AgentFlowDiagram.getJsonObj(obj.diagram) : undefined,
                'guardrails': obj.guardrails ?
                
                
                model.GuardrailsConfiguration.getJsonObj(obj.guardrails) : undefined,
                'sessionConfig': obj.sessionConfig ?
                
                
                model.SessionConfiguration.getJsonObj(obj.sessionConfig) : undefined,
                'agentCardConfig': obj.agentCardConfig ?
                
                
                model.AgentCardConfigDetail.getJsonObj(obj.agentCardConfig) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlow): object {
        const jsonObj = {...obj, ...{
            



















                    'diagram': obj.diagram ?
                
                
                model.AgentFlowDiagram.getDeserializedJsonObj(obj.diagram) : undefined,
                    'guardrails': obj.guardrails ?
                
                
                model.GuardrailsConfiguration.getDeserializedJsonObj(obj.guardrails) : undefined,
                    'sessionConfig': obj.sessionConfig ?
                
                
                model.SessionConfiguration.getDeserializedJsonObj(obj.sessionConfig) : undefined,
                    'agentCardConfig': obj.agentCardConfig ?
                
                
                model.AgentCardConfigDetail.getDeserializedJsonObj(obj.agentCardConfig) : undefined,

         }};

        
        
        return jsonObj;
    }
}
