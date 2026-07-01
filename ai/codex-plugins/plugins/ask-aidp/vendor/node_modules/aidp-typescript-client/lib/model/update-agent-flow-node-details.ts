// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Fields that can be updated in an existing node.
*/
export interface UpdateAgentFlowNodeDetails {
    /**
    * This field is deprecated. | It does not need to be set to any value for API calls.
    */
    'nodeType'?: string;
    /**
    * Name of this node.
    */
    'name'?: string;
    /**
    * Description of this node.
    */
    'description'?: string;
    /**
    * Canvas X coordinate. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'positionX'?: number;
    /**
    * Canvas Y coordinate. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'positionY'?: number;
    /**
    * Is node expanded in UI.
    */
    'isExpanded'?: boolean;
    /**
    * Parent node ID, for subgraphs.
    */
    'parentNodeId'?: string;
    /**
    * Configuration object for this node.
    */
    'configuration'?: { [key: string]: any; };
    /**
    * Definition ID for this node type.
    */
    'nodeTypeId'?: string;

   "type": string;
}

export namespace UpdateAgentFlowNodeDetails {










    export function getJsonObj(obj: UpdateAgentFlowNodeDetails): object {
        const jsonObj = {...obj, ...{
            









        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "PROMPT_TOOL":
                    return model.UpdatePromptToolNodeDetails.getJsonObj(<model.UpdatePromptToolNodeDetails>(<object>jsonObj), true);
                case "CUSTOM_TOOL":
                    return model.UpdateCustomToolNodeDetails.getJsonObj(<model.UpdateCustomToolNodeDetails>(<object>jsonObj), true);
                case "HTTP_TOOL":
                    return model.UpdateHttpToolNodeDetails.getJsonObj(<model.UpdateHttpToolNodeDetails>(<object>jsonObj), true);
                case "MCP_TOOL":
                    return model.UpdateMcpToolNodeDetails.getJsonObj(<model.UpdateMcpToolNodeDetails>(<object>jsonObj), true);
                case "RAG_TOOL":
                    return model.UpdateRagToolNodeDetails.getJsonObj(<model.UpdateRagToolNodeDetails>(<object>jsonObj), true);
                case "AGENT":
                    return model.UpdateAgentNodeDetails.getJsonObj(<model.UpdateAgentNodeDetails>(<object>jsonObj), true);
                case "SQL_TOOL":
                    return model.UpdateSqlToolNodeDetails.getJsonObj(<model.UpdateSqlToolNodeDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateAgentFlowNodeDetails): object {
        const jsonObj = {...obj, ...{
            









         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "PROMPT_TOOL":
                    return model.UpdatePromptToolNodeDetails.getDeserializedJsonObj(<model.UpdatePromptToolNodeDetails>(<object>jsonObj), true);
                case "CUSTOM_TOOL":
                    return model.UpdateCustomToolNodeDetails.getDeserializedJsonObj(<model.UpdateCustomToolNodeDetails>(<object>jsonObj), true);
                case "HTTP_TOOL":
                    return model.UpdateHttpToolNodeDetails.getDeserializedJsonObj(<model.UpdateHttpToolNodeDetails>(<object>jsonObj), true);
                case "MCP_TOOL":
                    return model.UpdateMcpToolNodeDetails.getDeserializedJsonObj(<model.UpdateMcpToolNodeDetails>(<object>jsonObj), true);
                case "RAG_TOOL":
                    return model.UpdateRagToolNodeDetails.getDeserializedJsonObj(<model.UpdateRagToolNodeDetails>(<object>jsonObj), true);
                case "AGENT":
                    return model.UpdateAgentNodeDetails.getDeserializedJsonObj(<model.UpdateAgentNodeDetails>(<object>jsonObj), true);
                case "SQL_TOOL":
                    return model.UpdateSqlToolNodeDetails.getDeserializedJsonObj(<model.UpdateSqlToolNodeDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
