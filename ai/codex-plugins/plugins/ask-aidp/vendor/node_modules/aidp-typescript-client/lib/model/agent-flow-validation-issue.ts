// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A validation issue for an Agent Flow diagram.
*/
export interface AgentFlowValidationIssue {
    /**
    * Stable machine-readable validation issue code.
    */
    'code': string;
    /**
    * Issue severity.
    */
    'severity': AgentFlowValidationIssue.Severity;
    /**
    * Validation phase that produced the issue.
    */
    'phase': AgentFlowValidationIssue.Phase;
    /**
    * Human-readable validation issue message.
    */
    'message': string;
    /**
    * JSON path or logical path to the invalid field.
    */
    'path'?: string;
    /**
    * Optional node key associated with the issue.
    */
    'nodeKey'?: string;
    /**
    * Optional edge key associated with the issue.
    */
    'edgeKey'?: string;
    /**
    * Optional tool, guardrails, catalog, or workspace key associated with the issue.
    */
    'resourceKey'?: string;
    /**
    * Optional user-facing remediation text.
    */
    'suggestedFix'?: string;
    /**
    * Non-sensitive issue details.
    */
    'details'?: { [key: string]: any; };

}

export namespace AgentFlowValidationIssue {


    export enum Severity {
    
    Error = "ERROR",
    Warning = "WARNING",
    Info = "INFO"

}


    export enum Phase {
    
    Model = "MODEL",
    Graph = "GRAPH",
    Config = "CONFIG",
    Reference = "REFERENCE",
    Codegen = "CODEGEN"

}









    export function getJsonObj(obj: AgentFlowValidationIssue): object {
        const jsonObj = {...obj, ...{
            










        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowValidationIssue): object {
        const jsonObj = {...obj, ...{
            










         }};

        
        
        return jsonObj;
    }
}
