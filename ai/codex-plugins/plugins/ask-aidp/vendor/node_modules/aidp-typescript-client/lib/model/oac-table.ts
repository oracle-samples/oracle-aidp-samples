// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table of an Oracle Analytics catalog.
*/
export interface OacTable extends model.Table {

   "entityType": string;
}

export namespace OacTable {

    export function getJsonObj(obj: OacTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getJsonObj(obj) as OacTable, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE_ANALYTICS';
    export function getDeserializedJsonObj(obj: OacTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getDeserializedJsonObj(obj) as OacTable, ...{
            
         }};

        
        
        return jsonObj;
    }
}
