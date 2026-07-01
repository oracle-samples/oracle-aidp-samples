// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for schema of a ADW external catalog.
*/
export interface AdwSchema extends model.Schema {

   "entityType": string;
}

export namespace AdwSchema {

    export function getJsonObj(obj: AdwSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getJsonObj(obj) as AdwSchema, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ADW';
    export function getDeserializedJsonObj(obj: AdwSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getDeserializedJsonObj(obj) as AdwSchema, ...{
            
         }};

        
        
        return jsonObj;
    }
}
