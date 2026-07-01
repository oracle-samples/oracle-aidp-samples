// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a Agent flow.
*/
export interface CreateAgentFlowDetails {
    /**
    * AgentFlow name.
    */
    'displayName': string;
    /**
    * AgentFlow description.
    */
    'description'?: string;
    /**
    * Path inside volume where the agentFlow json is written
    */
    'pathInfo': string;
    /**
    * The type of Agent Flow (Canvas or Code)
    */
    'type'?: CreateAgentFlowDetails.Type;
    /**
    * The path to project entry file
    */
    'entryFilePath'?: string;
    /**
    * The path to dependencies file
    */
    'dependenciesFilePath'?: string;
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'computeKey'?: string;
    'guardrails'?: model.GuardrailsConfiguration;
    'sessionConfig'?: model.SessionConfiguration;
    'agentCardConfig'?: model.AgentCardConfigDetail;
    'diagram'?: model.AgentFlowDiagram;

}

export namespace CreateAgentFlowDetails {




    export enum Type {
    
    Canvas = "CANVAS",
    Code = "CODE"

}









    export function getJsonObj(obj: CreateAgentFlowDetails): object {
        const jsonObj = {...obj, ...{
            







                'guardrails': obj.guardrails ?
                
                
                model.GuardrailsConfiguration.getJsonObj(obj.guardrails) : undefined,
                'sessionConfig': obj.sessionConfig ?
                
                
                model.SessionConfiguration.getJsonObj(obj.sessionConfig) : undefined,
                'agentCardConfig': obj.agentCardConfig ?
                
                
                model.AgentCardConfigDetail.getJsonObj(obj.agentCardConfig) : undefined,
                'diagram': obj.diagram ?
                
                
                model.AgentFlowDiagram.getJsonObj(obj.diagram) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateAgentFlowDetails): object {
        const jsonObj = {...obj, ...{
            







                    'guardrails': obj.guardrails ?
                
                
                model.GuardrailsConfiguration.getDeserializedJsonObj(obj.guardrails) : undefined,
                    'sessionConfig': obj.sessionConfig ?
                
                
                model.SessionConfiguration.getDeserializedJsonObj(obj.sessionConfig) : undefined,
                    'agentCardConfig': obj.agentCardConfig ?
                
                
                model.AgentCardConfigDetail.getDeserializedJsonObj(obj.agentCardConfig) : undefined,
                    'diagram': obj.diagram ?
                
                
                model.AgentFlowDiagram.getDeserializedJsonObj(obj.diagram) : undefined,
         }};

        
        
        return jsonObj;
    }
}
