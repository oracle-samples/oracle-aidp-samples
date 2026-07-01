// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary counts for Agent Flow diagram validation issues.
*/
export interface AgentFlowValidationSummary {
    /**
    * Total number of validation issues. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'totalIssues'?: number;
    /**
    * Number of error severity validation issues. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'errorCount'?: number;
    /**
    * Number of warning severity validation issues. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'warningCount'?: number;
    /**
    * Number of informational validation issues. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'infoCount'?: number;
    /**
    * Counts of validation issues by validation phase.
    */
    'phaseCounts'?: { [key: string]: number; };

}

export namespace AgentFlowValidationSummary {






    export function getJsonObj(obj: AgentFlowValidationSummary): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowValidationSummary): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
