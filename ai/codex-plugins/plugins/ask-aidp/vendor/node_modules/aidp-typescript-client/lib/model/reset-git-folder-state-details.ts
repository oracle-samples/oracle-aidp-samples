// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Action to either abort an in-progress merge/rebase or discard all local changes.
*/
export interface ResetGitFolderStateDetails {
    /**
    * The action to perform.
    */
    'action': ResetGitFolderStateDetails.Action;
    /**
    * List of file paths whose changes are to be discarded and reset to original.
    */
    'discardPaths'?: Array<string>;
    /**
    * If true with DISCARD, also remove ignored files (-x).
    */
    'canIncludeIgnored'?: boolean;

}

export namespace ResetGitFolderStateDetails {

    export enum Action {
    
    AbortMerge = "ABORT_MERGE",
    AbortRebase = "ABORT_REBASE",
    AbortPull = "ABORT_PULL",
    Discard = "DISCARD"

}




    export function getJsonObj(obj: ResetGitFolderStateDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ResetGitFolderStateDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
