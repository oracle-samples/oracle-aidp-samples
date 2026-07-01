// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Tool Node in an Agent Flow
*/
export interface SqlToolNode extends model.AgentFlowNode {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace SqlToolNode {


    export function getJsonObj(obj: SqlToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getJsonObj(obj) as SqlToolNode, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'SQL_TOOL';
    export function getDeserializedJsonObj(obj: SqlToolNode, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.AgentFlowNode.getDeserializedJsonObj(obj) as SqlToolNode, ...{
            

         }};

        
        
        return jsonObj;
    }
}
