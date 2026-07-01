// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table of a standard catalog.
*/
export interface StandardTable extends model.Table {

   "entityType": string;
}

export namespace StandardTable {

    export function getJsonObj(obj: StandardTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getJsonObj(obj) as StandardTable, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'STANDARD';
    export function getDeserializedJsonObj(obj: StandardTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getDeserializedJsonObj(obj) as StandardTable, ...{
            
         }};

        
        
        return jsonObj;
    }
}
