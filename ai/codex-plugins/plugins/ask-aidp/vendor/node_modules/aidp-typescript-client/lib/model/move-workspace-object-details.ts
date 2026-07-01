// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to move a workspace object to a different location.
*/
export interface MoveWorkspaceObjectDetails {
    /**
    * The fully qualified path of the workspace object that should be moved.
    */
    'fromPath'?: string;
    /**
    * The fully qualified destination path to which the Workspace object should be moved.
    */
    'toPath'?: string;

}

export namespace MoveWorkspaceObjectDetails {



    export function getJsonObj(obj: MoveWorkspaceObjectDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MoveWorkspaceObjectDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
