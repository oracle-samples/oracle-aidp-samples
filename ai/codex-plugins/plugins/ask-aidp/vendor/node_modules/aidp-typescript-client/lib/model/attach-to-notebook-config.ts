// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Configuration associated with the notebook being attached to the created cluster.
*/
export interface AttachToNotebookConfig {
    /**
    * Notebook URI path.
    */
    'notebookPath': string;

}

export namespace AttachToNotebookConfig {


    export function getJsonObj(obj: AttachToNotebookConfig): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AttachToNotebookConfig): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
