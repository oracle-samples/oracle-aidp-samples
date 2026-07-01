// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to create a new edge in a diagram.
* <p>
Excludes computed fields such as id, timeCreated, and timeUpdated.
* - srcNodeId: Source node identifier
* - destNodeId: Destination node identifier
* - srcNodeOutput: Source node's output port name
* - destNodeInput: Destination node's input port name
* 
*/
export interface CreateAgentFlowEdgeDetails {
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
    * Style/type for edge.
    */
    'edgeType'?: string;

}

export namespace CreateAgentFlowEdgeDetails {








    export function getJsonObj(obj: CreateAgentFlowEdgeDetails): object {
        const jsonObj = {...obj, ...{
            





                'edgeHandles': obj.edgeHandles ?
                
                obj.edgeHandles.map((item)=>{return model.Point.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateAgentFlowEdgeDetails): object {
        const jsonObj = {...obj, ...{
            





                    'edgeHandles': obj.edgeHandles ?
                
                obj.edgeHandles.map((item)=>{return model.Point.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
