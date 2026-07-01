// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Payload required to clear outputs of a notebook file.
*/
export interface ClearOutputsDetails {
    /**
    * To clear execution metadata like execution count, command_metadata etc.
    */
    'shouldClearExecMetadata'?: boolean;

}

export namespace ClearOutputsDetails {


    export function getJsonObj(obj: ClearOutputsDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClearOutputsDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
