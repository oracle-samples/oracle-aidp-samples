// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table summary of a Exadata catalog.
*/
export interface ExadataTableSummary extends model.TableSummary {

   "entityType": string;
}

export namespace ExadataTableSummary {

    export function getJsonObj(obj: ExadataTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getJsonObj(obj) as ExadataTableSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'EXADATA';
    export function getDeserializedJsonObj(obj: ExadataTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getDeserializedJsonObj(obj) as ExadataTableSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
