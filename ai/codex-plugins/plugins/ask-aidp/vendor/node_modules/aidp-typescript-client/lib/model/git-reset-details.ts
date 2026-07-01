// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details needed to reset the current branch for a folder context.
*/
export interface GitResetDetails {
    /**
    * Folder path used to locate the Git worktree.
    */
    'gitFolderPath'?: string;
    /**
    * Git branch name that is cloned.
    */
    'branchName'?: string;
    /**
    * Commit ID/ref to reset to. If omitted, server may default to HEAD~1 for soft reset flows.
    */
    'commitId'?: string;
    /**
    * Reset mode requested.
    */
    'resetMode'?: GitResetDetails.ResetMode;

}

export namespace GitResetDetails {




    export enum ResetMode {
    
    Soft = "SOFT",
    Mixed = "MIXED",
    Hard = "HARD"

}


    export function getJsonObj(obj: GitResetDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitResetDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
