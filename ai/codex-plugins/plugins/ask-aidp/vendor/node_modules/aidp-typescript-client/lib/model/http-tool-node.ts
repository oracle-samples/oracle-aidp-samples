// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Tool Node in an Agent Flow
*/
export interface HttpToolNode extends model.AgentFlowNode {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace HttpToolNode {


    export function getJsonObj(obj: HttpToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as HttpToolNode, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'HTTP_TOOL';
    export function getDeserializedJsonObj(obj: HttpToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as HttpToolNode, ...{
            

         }};

        
        
        return jsonObj;
    }
}
