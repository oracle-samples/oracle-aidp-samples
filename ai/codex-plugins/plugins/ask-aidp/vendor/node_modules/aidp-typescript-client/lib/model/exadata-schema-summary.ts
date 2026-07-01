// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of a Exadata external catalog.
*/
export interface ExadataSchemaSummary extends model.SchemaSummary {

   "entityType": string;
}

export namespace ExadataSchemaSummary {

    export function getJsonObj(obj: ExadataSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as ExadataSchemaSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'EXADATA';
    export function getDeserializedJsonObj(obj: ExadataSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as ExadataSchemaSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
