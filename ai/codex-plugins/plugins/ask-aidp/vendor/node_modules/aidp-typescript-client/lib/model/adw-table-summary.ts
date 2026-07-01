// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for table summary of a ADW catalog.
*/
export interface AdwTableSummary extends model.TableSummary {

   "entityType": string;
}

export namespace AdwTableSummary {

    export function getJsonObj(obj: AdwTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getJsonObj(obj) as AdwTableSummary, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const entityType = 'ADW';
    export function getDeserializedJsonObj(obj: AdwTableSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TableSummary.getDeserializedJsonObj(obj) as AdwTableSummary, ...{
            
         }};

        
        
        return jsonObj;
    }
}
