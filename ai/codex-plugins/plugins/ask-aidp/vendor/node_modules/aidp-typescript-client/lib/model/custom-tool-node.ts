// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A generic tool node in an Agent Flow
*/
export interface CustomToolNode extends model.AgentFlowNode {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace CustomToolNode {


    export function getJsonObj(obj: CustomToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as CustomToolNode, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'CUSTOM_TOOL';
    export function getDeserializedJsonObj(obj: CustomToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as CustomToolNode, ...{
            

         }};

        
        
        return jsonObj;
    }
}
