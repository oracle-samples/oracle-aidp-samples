// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details needed to repair Git work tree for a folder context.
*/
export interface GitRepairWorkTreeDetails {
    /**
    * Folder path used to locate the Git worktree.
    */
    'gitFolderPath': string;
    /**
    * Git branch name that is cloned.
    */
    'branchName': string;

}

export namespace GitRepairWorkTreeDetails {



    export function getJsonObj(obj: GitRepairWorkTreeDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitRepairWorkTreeDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
