// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for schema of a ATP external catalog.
*/
export interface AtpSchema extends model.Schema {

   "entityType": string;
}

export namespace AtpSchema {

    export function getJsonObj(obj: AtpSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getJsonObj(obj) as AtpSchema, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ATP';
    export function getDeserializedJsonObj(obj: AtpSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getDeserializedJsonObj(obj) as AtpSchema, ...{
            
         }};

        
        
        return jsonObj;
    }
}
