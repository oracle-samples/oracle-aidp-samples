// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a Agent flow.
*/
export interface UpdateAgentFlowDetails {
    /**
    * AgentFlow name.
    */
    'displayName'?: string;
    /**
    * AgentFlow description.
    */
    'description'?: string;
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'computeKey'?: string;
    'diagram'?: model.AgentFlowDiagram;
    /**
    * The path to project entry file
    */
    'entryFilePath'?: string;
    /**
    * The path to dependencies file
    */
    'dependenciesFilePath'?: string;
    /**
    * List of node keys that were removed from the flow.
    */
    'deletedNodes'?: Array<string>;
    /**
    * List of node keys that were updated in the flow.
    */
    'updatedNodes'?: Array<string>;
    /**
    * List of node keys that were newly added to the flow.
    */
    'addedNodes'?: Array<string>;
    'guardrails'?: model.GuardrailsConfiguration;
    'sessionConfig'?: model.SessionConfiguration;
    'agentCardConfig'?: model.AgentCardConfigDetail;

}

export namespace UpdateAgentFlowDetails {













    export function getJsonObj(obj: UpdateAgentFlowDetails): object {
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
    export function getDeserializedJsonObj(obj: UpdateAgentFlowDetails): object {
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
