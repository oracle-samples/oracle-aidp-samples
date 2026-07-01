// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table summary of a ALH catalog.
*/
export interface AlhTableSummary extends model.TableSummary {

   "entityType": string;
}

export namespace AlhTableSummary {

    export function getJsonObj(obj: AlhTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getJsonObj(obj) as AlhTableSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ALH';
    export function getDeserializedJsonObj(obj: AlhTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getDeserializedJsonObj(obj) as AlhTableSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
