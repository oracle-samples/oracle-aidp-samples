// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for schema of an Oracle Analytics external catalog.
*/
export interface OacSchema extends model.Schema {

   "entityType": string;
}

export namespace OacSchema {

    export function getJsonObj(obj: OacSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getJsonObj(obj) as OacSchema, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE_ANALYTICS';
    export function getDeserializedJsonObj(obj: OacSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getDeserializedJsonObj(obj) as OacSchema, ...{
            
         }};

        
        
        return jsonObj;
    }
}
