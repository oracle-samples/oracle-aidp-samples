// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update an HTTP Tool
*/
export interface UpdateHttpToolDetails extends model.UpdateToolDetails {
    /**
    * The list of template variable properties in the inputSchema
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.HttpToolConfiguration;

   "toolType": string;
}

export namespace UpdateHttpToolDetails {



    export function getJsonObj(obj: UpdateHttpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getJsonObj(obj) as UpdateHttpToolDetails, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'HTTP';
    export function getDeserializedJsonObj(obj: UpdateHttpToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getDeserializedJsonObj(obj) as UpdateHttpToolDetails, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.HttpToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
