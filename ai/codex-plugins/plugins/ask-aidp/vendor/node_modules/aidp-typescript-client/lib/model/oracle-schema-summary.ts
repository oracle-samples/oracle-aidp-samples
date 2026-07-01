// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of a Oracle external catalog.
*/
export interface OracleSchemaSummary extends model.SchemaSummary {

   "entityType": string;
}

export namespace OracleSchemaSummary {

    export function getJsonObj(obj: OracleSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as OracleSchemaSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE';
    export function getDeserializedJsonObj(obj: OracleSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as OracleSchemaSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
