// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to update a SQL Tool node
*/
export interface UpdateSqlToolNodeDetails extends model.UpdateAgentFlowNodeDetails {
    /**
    * The list of properties in the inputSchema, along with the default value and description of each property
    */
    'inputSchema'?: { [key: string]: any; };

   "type": string;
}

export namespace UpdateSqlToolNodeDetails {


    export function getJsonObj(obj: UpdateSqlToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getJsonObj(obj) as UpdateSqlToolNodeDetails, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'SQL_TOOL';
    export function getDeserializedJsonObj(obj: UpdateSqlToolNodeDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateAgentFlowNodeDetails.getDeserializedJsonObj(obj) as UpdateSqlToolNodeDetails, ...{
            

         }};

        
        
        return jsonObj;
    }
}
