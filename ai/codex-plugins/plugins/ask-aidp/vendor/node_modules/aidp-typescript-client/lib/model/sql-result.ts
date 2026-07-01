// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Rows for SQL result.
*/
export interface SqlResult {
    /**
    * Array of result rows with dynamic columns.
    */
    'rows'?: Array<{ [key: string]: string; }>;

}

export namespace SqlResult {


    export function getJsonObj(obj: SqlResult): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SqlResult): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
