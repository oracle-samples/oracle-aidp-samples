// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to rename a workspace object.
*/
export interface RenameWorkspaceObjectDetails {
    /**
    * The fully qualified path of the Workspace object that should be renamed.
    */
    'path'?: string;
    /**
    * The new name of the workspace object.
    */
    'newName'?: string;

}

export namespace RenameWorkspaceObjectDetails {



    export function getJsonObj(obj: RenameWorkspaceObjectDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RenameWorkspaceObjectDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
