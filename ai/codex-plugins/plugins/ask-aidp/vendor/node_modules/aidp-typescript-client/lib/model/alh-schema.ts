// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for schema of a ALH external catalog.
*/
export interface AlhSchema extends model.Schema {

   "entityType": string;
}

export namespace AlhSchema {

    export function getJsonObj(obj: AlhSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getJsonObj(obj) as AlhSchema, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ALH';
    export function getDeserializedJsonObj(obj: AlhSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getDeserializedJsonObj(obj) as AlhSchema, ...{
            
         }};

        
        
        return jsonObj;
    }
}
