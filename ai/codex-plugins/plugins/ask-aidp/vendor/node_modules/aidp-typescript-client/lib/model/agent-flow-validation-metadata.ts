// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Non-sensitive metadata for Agent Flow diagram validation.
*/
export interface AgentFlowValidationMetadata {
    /**
    * Whether deep validation was skipped.
    */
    'isDeepValidationSkipped'?: boolean;
    /**
    * Reason deep validation was skipped.
    */
    'skipReason'?: string;
    /**
    * Optional downstream LakeFlow request identifier.
    */
    'lakeFlowRequestId'?: string;
    /**
    * Validation duration in milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'durationInMillis'?: number;
    /**
    * Validation phases that were skipped.
    */
    'skippedPhases'?: Array<AgentFlowValidationMetadata.SkippedPhases>;
    /**
    * Validation rule durations in milliseconds.
    */
    'ruleDurationsInMillis'?: { [key: string]: number; };

}

export namespace AgentFlowValidationMetadata {





    export enum SkippedPhases {
    
    Model = "MODEL",
    Graph = "GRAPH",
    Config = "CONFIG",
    Reference = "REFERENCE",
    Codegen = "CODEGEN"

}



    export function getJsonObj(obj: AgentFlowValidationMetadata): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowValidationMetadata): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
