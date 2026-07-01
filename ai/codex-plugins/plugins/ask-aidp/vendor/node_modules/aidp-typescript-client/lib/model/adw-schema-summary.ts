// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of a ADW external catalog.
*/
export interface AdwSchemaSummary extends model.SchemaSummary {

   "entityType": string;
}

export namespace AdwSchemaSummary {

    export function getJsonObj(obj: AdwSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as AdwSchemaSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ADW';
    export function getDeserializedJsonObj(obj: AdwSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as AdwSchemaSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
