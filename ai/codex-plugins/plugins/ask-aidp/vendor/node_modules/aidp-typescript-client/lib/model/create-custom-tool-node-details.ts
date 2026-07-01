// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a Custom Tool Node in an Agent Flow
*/
export interface CreateCustomToolNodeDetails extends model.CreateAgentFlowNodeDetails {
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

export namespace CreateCustomToolNodeDetails {




    export function getJsonObj(obj: CreateCustomToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getJsonObj(obj) as CreateCustomToolNodeDetails, ...{
            


                'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'CUSTOM_TOOL';
    export function getDeserializedJsonObj(obj: CreateCustomToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as CreateCustomToolNodeDetails, ...{
            


                    'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
