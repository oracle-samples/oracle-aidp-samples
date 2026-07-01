// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a SQL Tool
*/
export interface CreateSqlToolDetails extends model.CreateToolDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.SqlToolConfiguration;

   "toolType": string;
}

export namespace CreateSqlToolDetails {



    export function getJsonObj(obj: CreateSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getJsonObj(obj) as CreateSqlToolDetails, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.SqlToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'SQL';
    export function getDeserializedJsonObj(obj: CreateSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateToolDetails.getDeserializedJsonObj(obj) as CreateSqlToolDetails, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.SqlToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
