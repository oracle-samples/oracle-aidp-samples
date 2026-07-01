// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The path details of init script.
*/
export interface InitScriptPath {
    /**
    * Full path of the init script file. Example - /Workspace/Shared/Folder1/my-init.sh or /Volumes/catalogName/schemaName/volumeName/Shared/Folder1/my-init.sh.
    */
    'destination': string;

}

export namespace InitScriptPath {


    export function getJsonObj(obj: InitScriptPath): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: InitScriptPath): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
