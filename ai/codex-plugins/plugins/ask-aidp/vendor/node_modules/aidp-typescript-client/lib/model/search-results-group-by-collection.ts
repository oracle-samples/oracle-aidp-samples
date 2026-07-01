// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Grouped list of objects by type.
*/
export interface SearchResultsGroupByCollection {
    /**
    * Total number of hits Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'count': number;
    /**
    * User search query
    */
    'query': string;
    /**
    * Objects grouped by their type. Each key is a type (e.g., \"database\", \"catalog\").
    */
    'items': { [key: string]: Array<model.ObjectSearchSummary>; };
    /**
    * Grouped doc name with count.
    */
    'aggregations': { [key: string]: Array<model.SearchObjectType>; };

}

export namespace SearchResultsGroupByCollection {





    export function getJsonObj(obj: SearchResultsGroupByCollection): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SearchResultsGroupByCollection): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
