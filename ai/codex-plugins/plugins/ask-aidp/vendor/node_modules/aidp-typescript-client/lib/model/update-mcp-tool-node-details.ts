// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a MCP Tool node
*/
export interface UpdateMcpToolNodeDetails extends model.UpdateAgentFlowNodeDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace UpdateMcpToolNodeDetails {


    export function getJsonObj(obj: UpdateMcpToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getJsonObj(obj) as UpdateMcpToolNodeDetails, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'MCP_TOOL';
    export function getDeserializedJsonObj(obj: UpdateMcpToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as UpdateMcpToolNodeDetails, ...{
            

         }};

        
        
        return jsonObj;
    }
}
