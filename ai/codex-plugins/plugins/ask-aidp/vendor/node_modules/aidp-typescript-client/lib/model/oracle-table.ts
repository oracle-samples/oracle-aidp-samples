// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table of a Oracle catalog.
*/
export interface OracleTable extends model.Table {

   "entityType": string;
}

export namespace OracleTable {

    export function getJsonObj(obj: OracleTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getJsonObj(obj) as OracleTable, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE';
    export function getDeserializedJsonObj(obj: OracleTable, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Table.getDeserializedJsonObj(obj) as OracleTable, ...{
            
         }};

        
        
        return jsonObj;
    }
}
