// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for validating an Agent Flow diagram without persisting changes.
*/
export interface ValidateAgentFlowDiagramDetails {
    /**
    * Optional Agent Flow key used to resolve persisted validation context.
    */
    'agentFlowKey'?: string;
    'agentFlowDiagram': model.AgentFlowDiagram;
    /**
    * The model and upgrade compatibility version for this validation request.
    */
    'modelVersion'?: string;
    /**
    * Validation depth to apply.
    */
    'validationLevel'?: ValidateAgentFlowDiagramDetails.ValidationLevel;
    'options'?: model.AgentFlowValidationOptions;

}

export namespace ValidateAgentFlowDiagramDetails {




    export enum ValidationLevel {
    
    Basic = "BASIC",
    Deep = "DEEP"

}



    export function getJsonObj(obj: ValidateAgentFlowDiagramDetails): object {
        const jsonObj = {...obj, ...{
            

                'agentFlowDiagram': obj.agentFlowDiagram ?
                
                
                model.AgentFlowDiagram.getJsonObj(obj.agentFlowDiagram) : undefined,


                'options': obj.options ?
                
                
                model.AgentFlowValidationOptions.getJsonObj(obj.options) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ValidateAgentFlowDiagramDetails): object {
        const jsonObj = {...obj, ...{
            

                    'agentFlowDiagram': obj.agentFlowDiagram ?
                
                
                model.AgentFlowDiagram.getDeserializedJsonObj(obj.agentFlowDiagram) : undefined,


                    'options': obj.options ?
                
                
                model.AgentFlowValidationOptions.getDeserializedJsonObj(obj.options) : undefined,
         }};

        
        
        return jsonObj;
    }
}
