// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table summary of a Oracle catalog.
*/
export interface OracleTableSummary extends model.TableSummary {

   "entityType": string;
}

export namespace OracleTableSummary {

    export function getJsonObj(obj: OracleTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getJsonObj(obj) as OracleTableSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE';
    export function getDeserializedJsonObj(obj: OracleTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getDeserializedJsonObj(obj) as OracleTableSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
