// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Range filters
*/
export interface RangeFilter {
    /**
    * Field Name for range filter
    */
    'field'?: model.RangeFilterEnum;
    /**
    * Field Name data type
    */
    'dataType'?: model.DataTypeEnum;
    /**
    * Start value
    */
    'start'?: string;
    /**
    * End value
    */
    'end'?: string;

}

export namespace RangeFilter {





    export function getJsonObj(obj: RangeFilter): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RangeFilter): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
