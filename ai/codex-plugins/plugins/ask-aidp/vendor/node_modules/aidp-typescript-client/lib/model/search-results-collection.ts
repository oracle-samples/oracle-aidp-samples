// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of objects
*/
export interface SearchResultsCollection {
    /**
    * Total number of hits Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'count': number;
    /**
    * User search query
    */
    'query': string;
    /**
    * List of objects.
    */
    'items': Array<model.ObjectSearchSummary>;
    /**
    * Grouped doc name with count.
    */
    'aggregations': { [key: string]: Array<model.SearchObjectType>; };

}

export namespace SearchResultsCollection {





    export function getJsonObj(obj: SearchResultsCollection): object {
        const jsonObj = {...obj, ...{
            


                'items': obj.items ?
                
                obj.items.map((item)=>{return model.ObjectSearchSummary.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SearchResultsCollection): object {
        const jsonObj = {...obj, ...{
            


                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.ObjectSearchSummary.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
