// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details for renaming column of a table.
*/
export interface RenameTableFieldDetails {
    /**
    * Name of the column.
    */
    'fieldName': string;
    /**
    * Updated name of the column.
    */
    'updatedFieldName': string;

}

export namespace RenameTableFieldDetails {



    export function getJsonObj(obj: RenameTableFieldDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RenameTableFieldDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
