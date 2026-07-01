// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table of a ADW catalog.
*/
export interface AdwTable extends model.Table {

   "entityType": string;
}

export namespace AdwTable {

    export function getJsonObj(obj: AdwTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getJsonObj(obj) as AdwTable, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ADW';
    export function getDeserializedJsonObj(obj: AdwTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getDeserializedJsonObj(obj) as AdwTable, ...{
            
         }};

        
        
        return jsonObj;
    }
}
