// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Object with sort criteria details
*/
export interface SortDetails {
    /**
    * Field name that needs to be sorted by.
    */
    'sortBy'?: model.SortFieldEnum;
    /**
    * Sort order for search results.
    */
    'sortOrder'?: SortDetails.SortOrder;

}

export namespace SortDetails {


    export enum SortOrder {
    
    Asc = "ASC",
    Desc = "DESC"

}


    export function getJsonObj(obj: SortDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SortDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
