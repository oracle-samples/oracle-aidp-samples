// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for schema of a standard catalog.
*/
export interface StandardSchemaSummary extends model.SchemaSummary {

   "entityType": string;
}

export namespace StandardSchemaSummary {

    export function getJsonObj(obj: StandardSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as StandardSchemaSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'STANDARD';
    export function getDeserializedJsonObj(obj: StandardSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as StandardSchemaSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
