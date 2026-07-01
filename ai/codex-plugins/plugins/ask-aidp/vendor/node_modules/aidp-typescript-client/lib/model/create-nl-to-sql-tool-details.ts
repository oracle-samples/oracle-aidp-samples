// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a NL to SQL Tool
*/
export interface CreateNlToSqlToolDetails extends model.CreateToolDetails {
    'inputSchema'?: model.NlToSqlToolInputSchema;
    'toolConfig'?: model.NlToSqlToolConfiguration;

   "toolType": string;
}

export namespace CreateNlToSqlToolDetails {



    export function getJsonObj(obj: CreateNlToSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getJsonObj(obj) as CreateNlToSqlToolDetails, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.NlToSqlToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
                'toolConfig': obj.toolConfig ?
                
                
                model.NlToSqlToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'NL2SQL';
    export function getDeserializedJsonObj(obj: CreateNlToSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getDeserializedJsonObj(obj) as CreateNlToSqlToolDetails, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.NlToSqlToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
                    'toolConfig': obj.toolConfig ?
                
                
                model.NlToSqlToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
