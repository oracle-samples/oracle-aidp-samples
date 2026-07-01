// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a Custom Tool
*/
export interface UpdateCustomToolNodeDetails extends model.UpdateAgentFlowNodeDetails {
    /**
    * The unique identifier (key) of the saved AI tool
    */
    'toolKey'?: string;
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.CustomToolConfiguration;

   "type": string;
}

export namespace UpdateCustomToolNodeDetails {




    export function getJsonObj(obj: UpdateCustomToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getJsonObj(obj) as UpdateCustomToolNodeDetails, ...{
            


                'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'CUSTOM_TOOL';
    export function getDeserializedJsonObj(obj: UpdateCustomToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as UpdateCustomToolNodeDetails, ...{
            


                    'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
