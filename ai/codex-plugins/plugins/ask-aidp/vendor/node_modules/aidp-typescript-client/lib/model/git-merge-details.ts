// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details needed to merge into the current branch for a folder context.
*/
export interface GitMergeDetails {
    /**
    * Folder path used to locate the Git worktree.
    */
    'gitFolderPath'?: string;
    /**
    * Git branch name that is cloned.
    */
    'branchName'?: string;
    /**
    * Remote branch name, if you want to merge a different branch.
    */
    'remoteBranchName': string;
    /**
    * Commit ID to merge (optional).
    */
    'commitId'?: string;

}

export namespace GitMergeDetails {





    export function getJsonObj(obj: GitMergeDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitMergeDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
