// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Pull remote changes for the given branch context (supports continue/abort during merge).
*/
export interface GitPullDetails {
    /**
    * Folder path used to locate the Git worktree.
    */
    'gitFolderPath'?: string;
    /**
    * Git branch name that is cloned.
    */
    'branchName'?: string;
    /**
    * Remote branch to pull from. Defaults to branchName if not provided.
    */
    'remoteBranchName'?: string;
    /**
    * Pull behavior requested by the caller.
    */
    'pullAction'?: GitPullDetails.PullAction;
    /**
    * Commit message used only when pullAction is MERGE_CONTINUE.
    */
    'commitMessage'?: string;

}

export namespace GitPullDetails {




    export enum PullAction {
    
    Pull = "PULL",
    MergeContinue = "MERGE_CONTINUE",
    MergeAbort = "MERGE_ABORT"

}



    export function getJsonObj(obj: GitPullDetails): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitPullDetails): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
