// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Base model for testing any tool.  {@code toolType} drives the subtype.
*/
export interface TestToolDetails {
    /**
    * Agent flow id for which the tool is being tested
    */
    'agentFlowId': string;

   "toolType": string;
}

export namespace TestToolDetails {


    export function getJsonObj(obj: TestToolDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "CUSTOM":
                    return model.TestCustomToolDetails.getJsonObj(<model.TestCustomToolDetails>(<object>jsonObj), true);
                case "HTTP":
                    return model.TestHttpToolDetails.getJsonObj(<model.TestHttpToolDetails>(<object>jsonObj), true);
                case "SQL":
                    return model.TestSqlToolDetails.getJsonObj(<model.TestSqlToolDetails>(<object>jsonObj), true);
                case "RAG":
                    return model.TestRagToolDetails.getJsonObj(<model.TestRagToolDetails>(<object>jsonObj), true);
                case "MCP":
                    return model.TestMcpToolDetails.getJsonObj(<model.TestMcpToolDetails>(<object>jsonObj), true);
                case "PROMPT":
                    return model.TestPromptToolDetails.getJsonObj(<model.TestPromptToolDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TestToolDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "CUSTOM":
                    return model.TestCustomToolDetails.getDeserializedJsonObj(<model.TestCustomToolDetails>(<object>jsonObj), true);
                case "HTTP":
                    return model.TestHttpToolDetails.getDeserializedJsonObj(<model.TestHttpToolDetails>(<object>jsonObj), true);
                case "SQL":
                    return model.TestSqlToolDetails.getDeserializedJsonObj(<model.TestSqlToolDetails>(<object>jsonObj), true);
                case "RAG":
                    return model.TestRagToolDetails.getDeserializedJsonObj(<model.TestRagToolDetails>(<object>jsonObj), true);
                case "MCP":
                    return model.TestMcpToolDetails.getDeserializedJsonObj(<model.TestMcpToolDetails>(<object>jsonObj), true);
                case "PROMPT":
                    return model.TestPromptToolDetails.getDeserializedJsonObj(<model.TestPromptToolDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)
        }
        }
        return jsonObj;
    }
}
