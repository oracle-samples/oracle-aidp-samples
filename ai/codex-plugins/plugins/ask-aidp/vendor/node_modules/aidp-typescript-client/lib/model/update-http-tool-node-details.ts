// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a HTTP Tool node
*/
export interface UpdateHttpToolNodeDetails extends model.UpdateAgentFlowNodeDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace UpdateHttpToolNodeDetails {


    export function getJsonObj(obj: UpdateHttpToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getJsonObj(obj) as UpdateHttpToolNodeDetails, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'HTTP_TOOL';
    export function getDeserializedJsonObj(obj: UpdateHttpToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as UpdateHttpToolNodeDetails, ...{
            

         }};

        
        
        return jsonObj;
    }
}
