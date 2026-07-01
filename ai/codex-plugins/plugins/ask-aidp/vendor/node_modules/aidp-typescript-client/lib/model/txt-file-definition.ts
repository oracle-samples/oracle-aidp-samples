// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Properties specific to a text file.
*/
export interface TxtFileDefinition {
    /**
    * Delimiter to be used with text file.
    */
    'delimiter'?: string;
    /**
    * Type of quote to be used with text file.
    */
    'quote'?: string;

}

export namespace TxtFileDefinition {



    export function getJsonObj(obj: TxtFileDefinition): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TxtFileDefinition): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
