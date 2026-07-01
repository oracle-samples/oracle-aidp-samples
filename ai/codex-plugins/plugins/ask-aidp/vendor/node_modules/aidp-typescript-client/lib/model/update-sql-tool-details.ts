// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a SQL Tool
*/
export interface UpdateSqlToolDetails extends model.UpdateToolDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.SqlToolConfiguration;

   "toolType": string;
}

export namespace UpdateSqlToolDetails {



    export function getJsonObj(obj: UpdateSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getJsonObj(obj) as UpdateSqlToolDetails, ...{
            

                'toolConfig': obj.toolConfig ?
                
                
                model.SqlToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const toolType = 'SQL';
    export function getDeserializedJsonObj(obj: UpdateSqlToolDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateToolDetails.getDeserializedJsonObj(obj) as UpdateSqlToolDetails, ...{
            

                    'toolConfig': obj.toolConfig ?
                
                
                model.SqlToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
