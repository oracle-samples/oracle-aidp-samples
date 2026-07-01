// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Base response for a test tool action.
*/
export interface TestToolResult {

   "toolType": string;
}

export namespace TestToolResult {

    export function getJsonObj(obj: TestToolResult): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "MCP":
                    return model.McpTestToolResult.getJsonObj(<model.McpTestToolResult>(<object>jsonObj), true);
                case "HTTP":
                    return model.HttpTestToolResult.getJsonObj(<model.HttpTestToolResult>(<object>jsonObj), true);
                case "RAG":
                    return model.RagTestToolResult.getJsonObj(<model.RagTestToolResult>(<object>jsonObj), true);
                case "SQL":
                    return model.SqlTestToolResult.getJsonObj(<model.SqlTestToolResult>(<object>jsonObj), true);
                case "PROMPT":
                    return model.PromptTestToolResult.getJsonObj(<model.PromptTestToolResult>(<object>jsonObj), true);
                case "CUSTOM":
                    return model.CustomTestToolResult.getJsonObj(<model.CustomTestToolResult>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TestToolResult): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "MCP":
                    return model.McpTestToolResult.getDeserializedJsonObj(<model.McpTestToolResult>(<object>jsonObj), true);
                case "HTTP":
                    return model.HttpTestToolResult.getDeserializedJsonObj(<model.HttpTestToolResult>(<object>jsonObj), true);
                case "RAG":
                    return model.RagTestToolResult.getDeserializedJsonObj(<model.RagTestToolResult>(<object>jsonObj), true);
                case "SQL":
                    return model.SqlTestToolResult.getDeserializedJsonObj(<model.SqlTestToolResult>(<object>jsonObj), true);
                case "PROMPT":
                    return model.PromptTestToolResult.getDeserializedJsonObj(<model.PromptTestToolResult>(<object>jsonObj), true);
                case "CUSTOM":
                    return model.CustomTestToolResult.getDeserializedJsonObj(<model.CustomTestToolResult>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)
        }
        }
        return jsonObj;
    }
}
