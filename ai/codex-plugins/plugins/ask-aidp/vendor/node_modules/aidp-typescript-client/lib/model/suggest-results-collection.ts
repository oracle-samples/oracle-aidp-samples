// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of suggested objects
*/
export interface SuggestResultsCollection {
    /**
    * Total number of hits Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'count': number;
    /**
    * User suggest query
    */
    'query': string;
    /**
    * List of objects.
    */
    'items': Array<model.ObjectSuggestSummary>;

}

export namespace SuggestResultsCollection {




    export function getJsonObj(obj: SuggestResultsCollection): object {
        const jsonObj = {...obj, ...{
            


                'items': obj.items ?
                
                obj.items.map((item)=>{return model.ObjectSuggestSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SuggestResultsCollection): object {
        const jsonObj = {...obj, ...{
            


                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.ObjectSuggestSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
