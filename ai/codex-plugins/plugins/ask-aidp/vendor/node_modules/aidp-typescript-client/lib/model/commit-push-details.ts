// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Commit staged/local changes and push to remote for the given branch context.
*/
export interface CommitPushDetails {
    /**
    * Folder path used to locate the Git worktree.
    */
    'gitFolderPath'?: string;
    /**
    * Git branch name that is cloned.
    */
    'branchName'?: string;
    /**
    * List of file paths (repo-relative) to stage before commit. If omitted, server may commit already-staged changes only.
    */
    'files'?: Array<string>;
    /**
    * Commit message.
    */
    'commitMessage': string;
    /**
    * Commit description.
    */
    'commitDescription'?: string;

}

export namespace CommitPushDetails {






    export function getJsonObj(obj: CommitPushDetails): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CommitPushDetails): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
