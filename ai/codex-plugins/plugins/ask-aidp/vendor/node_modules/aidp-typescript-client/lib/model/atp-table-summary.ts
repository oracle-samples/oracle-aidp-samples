// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table summary of a ATP catalog.
*/
export interface AtpTableSummary extends model.TableSummary {

   "entityType": string;
}

export namespace AtpTableSummary {

    export function getJsonObj(obj: AtpTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getJsonObj(obj) as AtpTableSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ATP';
    export function getDeserializedJsonObj(obj: AtpTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getDeserializedJsonObj(obj) as AtpTableSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
