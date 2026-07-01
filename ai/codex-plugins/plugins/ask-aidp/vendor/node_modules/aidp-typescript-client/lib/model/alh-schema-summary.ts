// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of a ALH external catalog.
*/
export interface AlhSchemaSummary extends model.SchemaSummary {

   "entityType": string;
}

export namespace AlhSchemaSummary {

    export function getJsonObj(obj: AlhSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as AlhSchemaSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ALH';
    export function getDeserializedJsonObj(obj: AlhSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as AlhSchemaSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
