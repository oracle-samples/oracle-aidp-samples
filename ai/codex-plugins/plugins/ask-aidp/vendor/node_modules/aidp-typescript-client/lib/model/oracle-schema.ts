// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for schema of a Oracle external catalog.
*/
export interface OracleSchema extends model.Schema {

   "entityType": string;
}

export namespace OracleSchema {

    export function getJsonObj(obj: OracleSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getJsonObj(obj) as OracleSchema, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE';
    export function getDeserializedJsonObj(obj: OracleSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getDeserializedJsonObj(obj) as OracleSchema, ...{
            
         }};

        
        
        return jsonObj;
    }
}
