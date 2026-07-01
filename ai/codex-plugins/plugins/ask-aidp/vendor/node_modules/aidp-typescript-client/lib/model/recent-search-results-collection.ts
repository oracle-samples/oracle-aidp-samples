// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of recent searches objects
*/
export interface RecentSearchResultsCollection {
    /**
    * Total number of items Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'count': number;
    /**
    * List of objects.
    */
    'items': Array<model.ObjectRecentSearchSummary>;

}

export namespace RecentSearchResultsCollection {



    export function getJsonObj(obj: RecentSearchResultsCollection): object {
        const jsonObj = {...obj, ...{
            

                'items': obj.items ?
                
                obj.items.map((item)=>{return model.ObjectRecentSearchSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RecentSearchResultsCollection): object {
        const jsonObj = {...obj, ...{
            

                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.ObjectRecentSearchSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
