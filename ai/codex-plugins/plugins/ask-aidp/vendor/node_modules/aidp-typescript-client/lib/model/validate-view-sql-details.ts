// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* View select query details.
*/
export interface ValidateViewSqlDetails {
    /**
    * The Query used to create the view.
    */
    'viewSelectQuery': string;

}

export namespace ValidateViewSqlDetails {


    export function getJsonObj(obj: ValidateViewSqlDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ValidateViewSqlDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
