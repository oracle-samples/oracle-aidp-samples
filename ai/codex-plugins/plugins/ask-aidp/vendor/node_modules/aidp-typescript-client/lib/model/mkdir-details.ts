// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to create a directory in the workspace.
*/
export interface MkdirDetails {
    /**
    * The fully qualified path of the directory to create.
    */
    'path'?: string;
    /**
    * The description of the directory to create.
    */
    'description'?: string;

}

export namespace MkdirDetails {



    export function getJsonObj(obj: MkdirDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MkdirDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
