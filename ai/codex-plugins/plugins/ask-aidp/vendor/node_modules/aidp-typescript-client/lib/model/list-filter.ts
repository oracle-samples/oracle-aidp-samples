// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List filters
*/
export interface ListFilter {
    /**
    * Field Name for term filter
    */
    'field'?: model.ListFilterEnum;
    /**
    * Field Name data type
    */
    'dataType'?: model.DataTypeEnum;
    /**
    * List of values
    */
    'values'?: Array<string>;

}

export namespace ListFilter {




    export function getJsonObj(obj: ListFilter): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ListFilter): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
