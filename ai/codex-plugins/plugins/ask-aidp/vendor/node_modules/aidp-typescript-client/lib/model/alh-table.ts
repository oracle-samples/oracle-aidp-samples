// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table of a ALH catalog.
*/
export interface AlhTable extends model.Table {

   "entityType": string;
}

export namespace AlhTable {

    export function getJsonObj(obj: AlhTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getJsonObj(obj) as AlhTable, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ALH';
    export function getDeserializedJsonObj(obj: AlhTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getDeserializedJsonObj(obj) as AlhTable, ...{
            
         }};

        
        
        return jsonObj;
    }
}
