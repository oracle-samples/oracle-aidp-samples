// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of a ATP external catalog.
*/
export interface AtpSchemaSummary extends model.SchemaSummary {

   "entityType": string;
}

export namespace AtpSchemaSummary {

    export function getJsonObj(obj: AtpSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as AtpSchemaSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ATP';
    export function getDeserializedJsonObj(obj: AtpSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as AtpSchemaSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
