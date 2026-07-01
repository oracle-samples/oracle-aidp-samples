// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table summary of an Oracle Analytics catalog.
*/
export interface OacTableSummary extends model.TableSummary {

   "entityType": string;
}

export namespace OacTableSummary {

    export function getJsonObj(obj: OacTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getJsonObj(obj) as OacTableSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ORACLE_ANALYTICS';
    export function getDeserializedJsonObj(obj: OacTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getDeserializedJsonObj(obj) as OacTableSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
