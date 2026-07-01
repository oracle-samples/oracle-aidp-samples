// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table of a ATP catalog.
*/
export interface AtpTable extends model.Table {

   "entityType": string;
}

export namespace AtpTable {

    export function getJsonObj(obj: AtpTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getJsonObj(obj) as AtpTable, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ATP';
    export function getDeserializedJsonObj(obj: AtpTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getDeserializedJsonObj(obj) as AtpTable, ...{
            
         }};

        
        
        return jsonObj;
    }
}
