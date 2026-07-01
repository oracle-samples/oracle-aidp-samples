// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Diagrammatic representation of the Agent Flow with all node and edge details
*/
export interface AgentFlowDiagram {
    /**
    * The unique identifier (UUID) of the Agent flow
    */
    'key'?: string;
    /**
    * AgentFlow name.
    */
    'displayName'?: string;
    /**
    * AgentFlow description.
    */
    'description'?: string;
    /**
    * The model and upgrade compatibility version for this agent flow diagram.
    */
    'modelVersion'?: string;
    /**
    * Mapping of nodeId to node objects.
    */
    'nodes'?: { [key: string]: model.AgentFlowNode; };
    /**
    * Mapping of edgeId to edge objects.
    */
    'edges'?: { [key: string]: model.AgentFlowEdge; };
    /**
    * A hash map with key=tool key, value=tool definition.   It is used to find the tool definition for a tool reference, where the tool is marked as a reference.
    */
    'toolsMap'?: { [key: string]: model.Tool; };
    /**
    * A hash map with key=guardrails config key, value=guardrails definition.   It is used to find the guardrails definition for a guardrails reference by name.
    */
    'guardrailsMap'?: { [key: string]: model.GuardrailsConfiguration; };

}

export namespace AgentFlowDiagram {









    export function getJsonObj(obj: AgentFlowDiagram): object {
        const jsonObj = {...obj, ...{
            




                'nodes': obj.nodes ?
                
                
                common.mapContainer(obj.nodes, model.AgentFlowNode.getJsonObj)
                 : undefined,
                'edges': obj.edges ?
                
                
                common.mapContainer(obj.edges, model.AgentFlowEdge.getJsonObj)
                 : undefined,
                'toolsMap': obj.toolsMap ?
                
                
                common.mapContainer(obj.toolsMap, model.Tool.getJsonObj)
                 : undefined,
                'guardrailsMap': obj.guardrailsMap ?
                
                
                common.mapContainer(obj.guardrailsMap, model.GuardrailsConfiguration.getJsonObj)
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowDiagram): object {
        const jsonObj = {...obj, ...{
            




                    'nodes': obj.nodes ?
                
                
                common.mapContainer(obj.nodes, model.AgentFlowNode.getDeserializedJsonObj)
                 : undefined,
                    'edges': obj.edges ?
                
                
                common.mapContainer(obj.edges, model.AgentFlowEdge.getDeserializedJsonObj)
                 : undefined,
                    'toolsMap': obj.toolsMap ?
                
                
                common.mapContainer(obj.toolsMap, model.Tool.getDeserializedJsonObj)
                 : undefined,
                    'guardrailsMap': obj.guardrailsMap ?
                
                
                common.mapContainer(obj.guardrailsMap, model.GuardrailsConfiguration.getDeserializedJsonObj)
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
