// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details for updating column of a table.
*/
export interface UpdateTableFieldDetails {
    /**
    * Name of the column.
    */
    'fieldName': string;
    /**
    * Description of the column.
    */
    'fieldDescription'?: string;

}

export namespace UpdateTableFieldDetails {



    export function getJsonObj(obj: UpdateTableFieldDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateTableFieldDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
