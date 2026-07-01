// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a NL to SQL Tool
*/
export interface UpdateNlToSqlToolDetails extends model.UpdateToolDetails {
    'inputSchema'?: model.NlToSqlToolInputSchema;
    'toolConfig'?: model.NlToSqlToolConfiguration;

   "toolType": string;
}

export namespace UpdateNlToSqlToolDetails {



    export function getJsonObj(obj: UpdateNlToSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getJsonObj(obj) as UpdateNlToSqlToolDetails, ...{
            
                'inputSchema': obj.inputSchema ?
                
                
                model.NlToSqlToolInputSchema.getJsonObj(obj.inputSchema) : undefined,
                'toolConfig': obj.toolConfig ?
                
                
                model.NlToSqlToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'NL2SQL';
    export function getDeserializedJsonObj(obj: UpdateNlToSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getDeserializedJsonObj(obj) as UpdateNlToSqlToolDetails, ...{
            
                    'inputSchema': obj.inputSchema ?
                
                
                model.NlToSqlToolInputSchema.getDeserializedJsonObj(obj.inputSchema) : undefined,
                    'toolConfig': obj.toolConfig ?
                
                
                model.NlToSqlToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
