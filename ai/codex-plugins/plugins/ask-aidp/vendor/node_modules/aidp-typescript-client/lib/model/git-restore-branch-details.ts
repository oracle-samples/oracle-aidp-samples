// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details needed to restore/discard local changes for a folder context.
*/
export interface GitRestoreBranchDetails {
    /**
    * Folder path used to locate the Git worktree.
    */
    'gitFolderPath'?: string;
    /**
    * Git branch name that is cloned.
    */
    'branchName'?: string;
    /**
    * List of files whose local/staged changes you want to discard (repo-relative paths).
    */
    'restoreFilesList': Array<string>;

}

export namespace GitRestoreBranchDetails {




    export function getJsonObj(obj: GitRestoreBranchDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitRestoreBranchDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
