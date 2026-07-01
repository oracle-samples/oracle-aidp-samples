// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of PAR URL associated with table.
*/
export interface ParDetails {
    /**
    * PAR URL of the table.
    */
    'parUrl': string;
    /**
    * The fully qualified name of the table in the format <catalog_name>.<schema_name>.<table_name>.
    */
    'tableKey': string;
    /**
    * The date and time the table was updated.
    */
    'timeExpires': Date;
    /**
    * The operation that can be performed on this resource.
    */
    'parAccessType': model.ParAccessType;

}

export namespace ParDetails {





    export function getJsonObj(obj: ParDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ParDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
