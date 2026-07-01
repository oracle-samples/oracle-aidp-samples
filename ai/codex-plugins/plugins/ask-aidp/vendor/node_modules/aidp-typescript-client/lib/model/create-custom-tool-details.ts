// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a Custom Tool
*/
export interface CreateCustomToolDetails extends model.CreateToolDetails {
    /**
    * The provider of the tool, default is AIDP
    */
    'toolProvider'?: string;
    /**
    * The type name for this tool
    */
    'toolTypeName'?: string;
    /**
    * The list of named properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.CustomToolConfiguration;

   "toolType": string;
}

export namespace CreateCustomToolDetails {





    export function getJsonObj(obj: CreateCustomToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getJsonObj(obj) as CreateCustomToolDetails, ...{
            



                'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'CUSTOM';
    export function getDeserializedJsonObj(obj: CreateCustomToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getDeserializedJsonObj(obj) as CreateCustomToolDetails, ...{
            



                    'toolConfig': obj.toolConfig ?
                
                
                model.CustomToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
