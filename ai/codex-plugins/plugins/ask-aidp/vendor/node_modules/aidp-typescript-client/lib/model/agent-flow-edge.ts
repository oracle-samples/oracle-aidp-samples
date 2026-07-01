// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An edge connecting two nodes in the diagram, with context and metadata.
*/
export interface AgentFlowEdge {
    /**
    * Source node for this edge.
    */
    'srcNodeId': string;
    /**
    * Destination node for this edge.
    */
    'destNodeId': string;
    /**
    * Output port on the source node.
    */
    'srcNodeOutput': string;
    /**
    * Input port on the destination node.
    */
    'destNodeInput': string;
    /**
    * Parent node, if hierarchical.
    */
    'parentNodeId'?: string;
    /**
    * Geometry handle coordinates.
    */
    'edgeHandles'?: Array<model.Point>;
    /**
    * Extensible Properties of the Edge
    */
    'edgeProperties'?: { [key: string]: any; };
    /**
    * Style/type for edge.
    */
    'edgeType'?: string;
    /**
    * Unique edge identifier.
    */
    'key': string;
    /**
    * RFC3339 timestamp when edge was created.
    */
    'timeCreated'?: Date;
    /**
    * RFC3339 timestamp when edge was last updated.
    */
    'timeUpdated'?: Date;
    /**
    * List of validation errors encountered in the diagram.
    */
    'validationErrors'?: Array<model.ValidationError>;

}

export namespace AgentFlowEdge {













    export function getJsonObj(obj: AgentFlowEdge): object {
        const jsonObj = {...obj, ...{
            





                'edgeHandles': obj.edgeHandles ?
                
                obj.edgeHandles.map((item)=>{return model.Point.getJsonObj(item)})
                
                 : undefined,





                'validationErrors': obj.validationErrors ?
                
                obj.validationErrors.map((item)=>{return model.ValidationError.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowEdge): object {
        const jsonObj = {...obj, ...{
            





                    'edgeHandles': obj.edgeHandles ?
                
                obj.edgeHandles.map((item)=>{return model.Point.getDeserializedJsonObj(item)})
                
                 : undefined,





                    'validationErrors': obj.validationErrors ?
                
                obj.validationErrors.map((item)=>{return model.ValidationError.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
