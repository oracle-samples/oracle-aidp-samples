// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Column inferred from location.
*/
export interface InferSchemaColumn {
    /**
    * Name of the column.
    */
    'fieldName'?: string;
    /**
    * Type of the column.
    */
    'fieldType'?: string;
    /**
    * Boolean value indicating if this column is partition column.
    */
    'isPartition'?: boolean;
    /**
    * For partition columns rank value indicates level, for non-partition column the value will be zero. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'rank'?: number;

}

export namespace InferSchemaColumn {





    export function getJsonObj(obj: InferSchemaColumn): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: InferSchemaColumn): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
