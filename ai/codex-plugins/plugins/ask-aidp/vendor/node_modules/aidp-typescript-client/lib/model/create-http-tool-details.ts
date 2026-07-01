// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create an HTTP Tool
*/
export interface CreateHttpToolDetails extends model.CreateToolDetails {
    /**
    * The list of template variable properties in the inputSchema
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.HttpToolConfiguration;

   "toolType": string;
}

export namespace CreateHttpToolDetails {



    export function getJsonObj(obj: CreateHttpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getJsonObj(obj) as CreateHttpToolDetails, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'HTTP';
    export function getDeserializedJsonObj(obj: CreateHttpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getDeserializedJsonObj(obj) as CreateHttpToolDetails, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
