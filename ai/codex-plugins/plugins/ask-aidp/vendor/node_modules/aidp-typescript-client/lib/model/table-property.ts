// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The property of the table.
*/
export interface TableProperty {
    /**
    * Property name.
    */
    'propertyName': string;
    /**
    * Property value.
    */
    'propertyValue'?: string;

}

export namespace TableProperty {



    export function getJsonObj(obj: TableProperty): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TableProperty): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
