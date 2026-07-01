// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details about the new managed table.
*/
export interface ManagedTableDefinition {
    /**
    * Data format of the managed table.
    */
    'managedTableDataFormat': model.DataFormat;

}

export namespace ManagedTableDefinition {


    export function getJsonObj(obj: ManagedTableDefinition): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManagedTableDefinition): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
