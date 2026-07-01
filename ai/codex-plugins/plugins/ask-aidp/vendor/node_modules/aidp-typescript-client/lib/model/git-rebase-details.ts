// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details needed to perform a Git rebase for a folder context.
*/
export interface GitRebaseDetails {
    /**
    * Folder path used to locate the Git worktree.
    */
    'gitFolderPath'?: string;
    /**
    * Git branch name that is cloned.
    */
    'branchName'?: string;
    /**
    * Remote branch name to rebase onto (optional).
    */
    'remoteBranchName': string;
    /**
    * Commit ID to rebase onto (optional).
    */
    'commitId'?: string;

}

export namespace GitRebaseDetails {





    export function getJsonObj(obj: GitRebaseDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitRebaseDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
