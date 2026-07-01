// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of an Oracle Analytics external catalog.
*/
export interface OacSchemaSummary extends model.SchemaSummary {

   "entityType": string;
}

export namespace OacSchemaSummary {

    export function getJsonObj(obj: OacSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as OacSchemaSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE_ANALYTICS';
    export function getDeserializedJsonObj(obj: OacSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as OacSchemaSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
