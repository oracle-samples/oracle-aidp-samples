// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a HTTP Tool Node in an Agent Flow
*/
export interface CreateHttpToolNodeDetails extends model.CreateAgentFlowNodeDetails {
    /**
    * The unique identifier (key) of the saved AI tool
    */
    'toolKey'?: string;
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.HttpToolConfiguration;

   "type": string;
}

export namespace CreateHttpToolNodeDetails {




    export function getJsonObj(obj: CreateHttpToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getJsonObj(obj) as CreateHttpToolNodeDetails, ...{
            


                'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'HTTP_TOOL';
    export function getDeserializedJsonObj(obj: CreateHttpToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as CreateHttpToolNodeDetails, ...{
            


                    'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
