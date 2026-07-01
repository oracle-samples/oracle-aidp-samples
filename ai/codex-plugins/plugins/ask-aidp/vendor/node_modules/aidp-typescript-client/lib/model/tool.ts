// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A tool is a function that an agent can call. Tools can retrieve data, call external APIs, HTTP endpoints, execute a snippet of code, entire Python scripts, etc. | A tool is stateless, doesn't reason  (it just executes), and can be reused across agents.
*/
export interface Tool {
    /**
    * The unique identifier of the tool
    */
    'key'?: string;
    /**
    * Tool name.
    */
    'displayName'?: string;
    /**
    * The key of the Workspace to which this tool belongs.
    */
    'workspaceKey'?: string;
    /**
    * Tool description.
    */
    'description'?: string;
    /**
    * Canvas X coordinate of the Tool node Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'positionX'?: number;
    /**
    * Canvas Y coordinate of the Tool node Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'positionY'?: number;
    /**
    * A list of key-value pairs to use for configuring the tool
    */
    'properties'?: { [key: string]: any; };
    /**
    * The date and time the tool was created.
    */
    'timeCreated'?: Date;
    /**
    * The date and time the tool was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The OCID of the user/principal who created the tool.
    */
    'createdBy'?: string;
    /**
    * The ID of the user who last updated the schema.
    */
    'updatedBy'?: string;

   "toolType": string;
}

export namespace Tool {












    export function getJsonObj(obj: Tool): object {
        const jsonObj = {...obj, ...{
            











        }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "CUSTOM":
                    return model.CustomTool.getJsonObj(<model.CustomTool>(<object>jsonObj), true);
                case "PROMPT":
                    return model.PromptTool.getJsonObj(<model.PromptTool>(<object>jsonObj), true);
                case "MCP":
                    return model.McpTool.getJsonObj(<model.McpTool>(<object>jsonObj), true);
                case "SQL":
                    return model.SqlTool.getJsonObj(<model.SqlTool>(<object>jsonObj), true);
                case "RAG":
                    return model.RagTool.getJsonObj(<model.RagTool>(<object>jsonObj), true);
                case "HTTP":
                    return model.HttpTool.getJsonObj(<model.HttpTool>(<object>jsonObj), true);
                case "NL2SQL":
                    return model.NlToSqlTool.getJsonObj(<model.NlToSqlTool>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Tool): object {
        const jsonObj = {...obj, ...{
            











         }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "CUSTOM":
                    return model.CustomTool.getDeserializedJsonObj(<model.CustomTool>(<object>jsonObj), true);
                case "PROMPT":
                    return model.PromptTool.getDeserializedJsonObj(<model.PromptTool>(<object>jsonObj), true);
                case "MCP":
                    return model.McpTool.getDeserializedJsonObj(<model.McpTool>(<object>jsonObj), true);
                case "SQL":
                    return model.SqlTool.getDeserializedJsonObj(<model.SqlTool>(<object>jsonObj), true);
                case "RAG":
                    return model.RagTool.getDeserializedJsonObj(<model.RagTool>(<object>jsonObj), true);
                case "HTTP":
                    return model.HttpTool.getDeserializedJsonObj(<model.HttpTool>(<object>jsonObj), true);
                case "NL2SQL":
                    return model.NlToSqlTool.getDeserializedJsonObj(<model.NlToSqlTool>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)
        }
        }
        return jsonObj;
    }
}
