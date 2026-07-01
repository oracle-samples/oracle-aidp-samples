// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table of a Exadata catalog.
*/
export interface ExadataTable extends model.Table {

   "entityType": string;
}

export namespace ExadataTable {

    export function getJsonObj(obj: ExadataTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getJsonObj(obj) as ExadataTable, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'EXADATA';
    export function getDeserializedJsonObj(obj: ExadataTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getDeserializedJsonObj(obj) as ExadataTable, ...{
            
         }};

        
        
        return jsonObj;
    }
}
