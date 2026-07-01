// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A node in a diagram, representing logic, data, or operation.
*/
export interface AgentFlowNode {
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
    /**
    * Unique identifier for the node.
    */
    'key': string;
    /**
    * RFC3339 timestamp when node was created.
    */
    'timeCreated'?: Date;
    /**
    * RFC3339 timestamp when node was last updated.
    */
    'timeUpdated'?: Date;
    /**
    * Array of NodeInput objects.
    */
    'inputs'?: Array<model.NodeInput>;
    /**
    * Array of NodeOutput objects.
    */
    'outputs'?: Array<model.NodeOutput>;
    /**
    * List of validation errors encountered in the diagram.
    */
    'validationErrors'?: Array<model.ValidationError>;

   "type": string;
}

export namespace AgentFlowNode {
















    export function getJsonObj(obj: AgentFlowNode): object {
        const jsonObj = {...obj, ...{
            












                'inputs': obj.inputs ?
                
                obj.inputs.map((item)=>{return model.NodeInput.getJsonObj(item)})
                
                 : undefined,
                'outputs': obj.outputs ?
                
                obj.outputs.map((item)=>{return model.NodeOutput.getJsonObj(item)})
                
                 : undefined,
                'validationErrors': obj.validationErrors ?
                
                obj.validationErrors.map((item)=>{return model.ValidationError.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "START_NODE":
                    return model.StartNode.getJsonObj(<model.StartNode>(<object>jsonObj), true);
                case "HTTP_TOOL":
                    return model.HttpToolNode.getJsonObj(<model.HttpToolNode>(<object>jsonObj), true);
                case "CUSTOM_TOOL":
                    return model.CustomToolNode.getJsonObj(<model.CustomToolNode>(<object>jsonObj), true);
                case "RAG_TOOL":
                    return model.RagToolNode.getJsonObj(<model.RagToolNode>(<object>jsonObj), true);
                case "HUMAN_IN_THE_LOOP":
                    return model.HumanInTheLoopNode.getJsonObj(<model.HumanInTheLoopNode>(<object>jsonObj), true);
                case "AGENT":
                    return model.AgentNode.getJsonObj(<model.AgentNode>(<object>jsonObj), true);
                case "MCP_TOOL":
                    return model.McpToolNode.getJsonObj(<model.McpToolNode>(<object>jsonObj), true);
                case "EXTERNAL_AGENT":
                    return model.ExternalAgentNode.getJsonObj(<model.ExternalAgentNode>(<object>jsonObj), true);
                case "SQL_TOOL":
                    return model.SqlToolNode.getJsonObj(<model.SqlToolNode>(<object>jsonObj), true);
                case "SUPERVISOR_AGENT":
                    return model.SupervisorAgentNode.getJsonObj(<model.SupervisorAgentNode>(<object>jsonObj), true);
                case "NESTED_AGENT_FLOW":
                    return model.NestedAgentFlowNode.getJsonObj(<model.NestedAgentFlowNode>(<object>jsonObj), true);
                case "PROMPT_TOOL":
                    return model.PromptToolNode.getJsonObj(<model.PromptToolNode>(<object>jsonObj), true);
                case "GUARDRAILS":
                    return model.GuardrailNode.getJsonObj(<model.GuardrailNode>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowNode): object {
        const jsonObj = {...obj, ...{
            












                    'inputs': obj.inputs ?
                
                obj.inputs.map((item)=>{return model.NodeInput.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'outputs': obj.outputs ?
                
                obj.outputs.map((item)=>{return model.NodeOutput.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'validationErrors': obj.validationErrors ?
                
                obj.validationErrors.map((item)=>{return model.ValidationError.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "START_NODE":
                    return model.StartNode.getDeserializedJsonObj(<model.StartNode>(<object>jsonObj), true);
                case "HTTP_TOOL":
                    return model.HttpToolNode.getDeserializedJsonObj(<model.HttpToolNode>(<object>jsonObj), true);
                case "CUSTOM_TOOL":
                    return model.CustomToolNode.getDeserializedJsonObj(<model.CustomToolNode>(<object>jsonObj), true);
                case "RAG_TOOL":
                    return model.RagToolNode.getDeserializedJsonObj(<model.RagToolNode>(<object>jsonObj), true);
                case "HUMAN_IN_THE_LOOP":
                    return model.HumanInTheLoopNode.getDeserializedJsonObj(<model.HumanInTheLoopNode>(<object>jsonObj), true);
                case "AGENT":
                    return model.AgentNode.getDeserializedJsonObj(<model.AgentNode>(<object>jsonObj), true);
                case "MCP_TOOL":
                    return model.McpToolNode.getDeserializedJsonObj(<model.McpToolNode>(<object>jsonObj), true);
                case "EXTERNAL_AGENT":
                    return model.ExternalAgentNode.getDeserializedJsonObj(<model.ExternalAgentNode>(<object>jsonObj), true);
                case "SQL_TOOL":
                    return model.SqlToolNode.getDeserializedJsonObj(<model.SqlToolNode>(<object>jsonObj), true);
                case "SUPERVISOR_AGENT":
                    return model.SupervisorAgentNode.getDeserializedJsonObj(<model.SupervisorAgentNode>(<object>jsonObj), true);
                case "NESTED_AGENT_FLOW":
                    return model.NestedAgentFlowNode.getDeserializedJsonObj(<model.NestedAgentFlowNode>(<object>jsonObj), true);
                case "PROMPT_TOOL":
                    return model.PromptToolNode.getDeserializedJsonObj(<model.PromptToolNode>(<object>jsonObj), true);
                case "GUARDRAILS":
                    return model.GuardrailNode.getDeserializedJsonObj(<model.GuardrailNode>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
