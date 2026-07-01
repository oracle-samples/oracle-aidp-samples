// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Recent Search object in a Data Lake
*/
export interface ObjectRecentSearchSummary {
    /**
    * De-normalized search term.
    */
    'displayName': string;

}

export namespace ObjectRecentSearchSummary {


    export function getJsonObj(obj: ObjectRecentSearchSummary): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ObjectRecentSearchSummary): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
