// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of validating an Agent Flow diagram.
*/
export interface AgentFlowValidationResult {
    /**
    * True when the diagram has no error severity validation issues.
    */
    'isValid': boolean;
    /**
    * Highest validation level completed.
    */
    'validationLevel': AgentFlowValidationResult.ValidationLevel;
    'summary'?: model.AgentFlowValidationSummary;
    /**
    * Ordered validation issue list.
    */
    'issues'?: Array<model.AgentFlowValidationIssue>;
    'metadata'?: model.AgentFlowValidationMetadata;

}

export namespace AgentFlowValidationResult {


    export enum ValidationLevel {
    
    Basic = "BASIC",
    Deep = "DEEP"

}





    export function getJsonObj(obj: AgentFlowValidationResult): object {
        const jsonObj = {...obj, ...{
            


                'summary': obj.summary ?
                
                
                model.AgentFlowValidationSummary.getJsonObj(obj.summary) : undefined,
                'issues': obj.issues ?
                
                obj.issues.map((item)=>{return model.AgentFlowValidationIssue.getJsonObj(item)})
                
                 : undefined,
                'metadata': obj.metadata ?
                
                
                model.AgentFlowValidationMetadata.getJsonObj(obj.metadata) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowValidationResult): object {
        const jsonObj = {...obj, ...{
            


                    'summary': obj.summary ?
                
                
                model.AgentFlowValidationSummary.getDeserializedJsonObj(obj.summary) : undefined,
                    'issues': obj.issues ?
                
                obj.issues.map((item)=>{return model.AgentFlowValidationIssue.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'metadata': obj.metadata ?
                
                
                model.AgentFlowValidationMetadata.getDeserializedJsonObj(obj.metadata) : undefined,
         }};

        
        
        return jsonObj;
    }
}
