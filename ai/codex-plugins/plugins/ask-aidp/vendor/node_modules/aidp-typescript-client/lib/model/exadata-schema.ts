// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for schema of a Exadata external catalog.
*/
export interface ExadataSchema extends model.Schema {

   "entityType": string;
}

export namespace ExadataSchema {

    export function getJsonObj(obj: ExadataSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getJsonObj(obj) as ExadataSchema, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'EXADATA';
    export function getDeserializedJsonObj(obj: ExadataSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getDeserializedJsonObj(obj) as ExadataSchema, ...{
            
         }};

        
        
        return jsonObj;
    }
}
