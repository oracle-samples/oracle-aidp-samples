// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a SQL Tool Node in an Agent Flow
*/
export interface CreateSqlToolNodeDetails extends model.CreateAgentFlowNodeDetails {
    /**
    * The unique identifier (key) of the saved AI tool
    */
    'toolKey'?: string;
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };
    'toolConfig'?: model.SqlToolConfiguration;

   "type": string;
}

export namespace CreateSqlToolNodeDetails {




    export function getJsonObj(obj: CreateSqlToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getJsonObj(obj) as CreateSqlToolNodeDetails, ...{
            


                'toolConfig': obj.toolConfig ?
                
                
                model.SqlToolConfiguration.getJsonObj(obj.toolConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'SQL_TOOL';
    export function getDeserializedJsonObj(obj: CreateSqlToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as CreateSqlToolNodeDetails, ...{
            


                    'toolConfig': obj.toolConfig ?
                
                
                model.SqlToolConfiguration.getDeserializedJsonObj(obj.toolConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
