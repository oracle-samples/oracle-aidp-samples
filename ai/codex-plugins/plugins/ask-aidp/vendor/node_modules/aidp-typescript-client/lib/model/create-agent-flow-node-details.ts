// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to create a new node in a diagram.
*/
export interface CreateAgentFlowNodeDetails {
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
    * Source node to which this node is connected
    */
    'srcNodeId'?: string;
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

export namespace CreateAgentFlowNodeDetails {











    export function getJsonObj(obj: CreateAgentFlowNodeDetails): object {
        const jsonObj = {...obj, ...{
            










        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "CUSTOM_TOOL":
                    return model.CreateCustomToolNodeDetails.getJsonObj(<model.CreateCustomToolNodeDetails>(<object>jsonObj), true);
                case "MCP_TOOL":
                    return model.CreateMcpToolNodeDetails.getJsonObj(<model.CreateMcpToolNodeDetails>(<object>jsonObj), true);
                case "PROMPT_TOOL":
                    return model.CreatePromptToolNodeDetails.getJsonObj(<model.CreatePromptToolNodeDetails>(<object>jsonObj), true);
                case "HTTP_TOOL":
                    return model.CreateHttpToolNodeDetails.getJsonObj(<model.CreateHttpToolNodeDetails>(<object>jsonObj), true);
                case "AGENT":
                    return model.CreateAgentNodeDetails.getJsonObj(<model.CreateAgentNodeDetails>(<object>jsonObj), true);
                case "RAG_TOOL":
                    return model.CreateRagToolNodeDetails.getJsonObj(<model.CreateRagToolNodeDetails>(<object>jsonObj), true);
                case "SQL_TOOL":
                    return model.CreateSqlToolNodeDetails.getJsonObj(<model.CreateSqlToolNodeDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateAgentFlowNodeDetails): object {
        const jsonObj = {...obj, ...{
            










         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "CUSTOM_TOOL":
                    return model.CreateCustomToolNodeDetails.getDeserializedJsonObj(<model.CreateCustomToolNodeDetails>(<object>jsonObj), true);
                case "MCP_TOOL":
                    return model.CreateMcpToolNodeDetails.getDeserializedJsonObj(<model.CreateMcpToolNodeDetails>(<object>jsonObj), true);
                case "PROMPT_TOOL":
                    return model.CreatePromptToolNodeDetails.getDeserializedJsonObj(<model.CreatePromptToolNodeDetails>(<object>jsonObj), true);
                case "HTTP_TOOL":
                    return model.CreateHttpToolNodeDetails.getDeserializedJsonObj(<model.CreateHttpToolNodeDetails>(<object>jsonObj), true);
                case "AGENT":
                    return model.CreateAgentNodeDetails.getDeserializedJsonObj(<model.CreateAgentNodeDetails>(<object>jsonObj), true);
                case "RAG_TOOL":
                    return model.CreateRagToolNodeDetails.getDeserializedJsonObj(<model.CreateRagToolNodeDetails>(<object>jsonObj), true);
                case "SQL_TOOL":
                    return model.CreateSqlToolNodeDetails.getDeserializedJsonObj(<model.CreateSqlToolNodeDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
