// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Columns for view.
*/
export interface ViewFieldDetails {
    /**
    * Name of the column.
    */
    'fieldName': string;
    /**
    * Type of the column.
    */
    'fieldType'?: string;
    /**
    * Precision of the column.
    */
    'fieldPrecision'?: string;
    /**
    * Scale of the column.
    */
    'fieldScale'?: string;
    /**
    * Description of the column.
    */
    'fieldDescription'?: string;

}

export namespace ViewFieldDetails {






    export function getJsonObj(obj: ViewFieldDetails): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ViewFieldDetails): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
