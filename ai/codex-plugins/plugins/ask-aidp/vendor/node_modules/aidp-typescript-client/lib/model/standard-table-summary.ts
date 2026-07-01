// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table summary of a standard catalog.
*/
export interface StandardTableSummary extends model.TableSummary {

   "entityType": string;
}

export namespace StandardTableSummary {

    export function getJsonObj(obj: StandardTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getJsonObj(obj) as StandardTableSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'STANDARD';
    export function getDeserializedJsonObj(obj: StandardTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getDeserializedJsonObj(obj) as StandardTableSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
