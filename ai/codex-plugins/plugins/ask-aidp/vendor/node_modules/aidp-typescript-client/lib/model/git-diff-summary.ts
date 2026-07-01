// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a file that differs in a Git folder context.
*/
export interface GitDiffSummary {
    /**
    * File path relative to repository root.
    */
    'gitFilePath': string;
    /**
    * Type of change for this file.
    */
    'changeType': GitDiffSummary.ChangeType;
    /**
    * Previous path if the file was renamed.
    */
    'oldGitFilePath'?: string;
    /**
    * Whether the file is currently in an unmerged/conflicted state.
    */
    'isConflict': boolean;
    /**
    * Conflict classification when isConflict is true.
    */
    'conflictType'?: GitDiffSummary.ConflictType;

}

export namespace GitDiffSummary {


    export enum ChangeType {
    
    Added = "ADDED",
    Modified = "MODIFIED",
    Deleted = "DELETED",
    Renamed = "RENAMED",
    Copied = "COPIED",
    TypeChanged = "TYPE_CHANGED",
    Unmerged = "UNMERGED",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}




    export enum ConflictType {
    
    BothModified = "BOTH_MODIFIED",
    BothAdded = "BOTH_ADDED",
    BothDeleted = "BOTH_DELETED",
    AddedByUs = "ADDED_BY_US",
    AddedByThem = "ADDED_BY_THEM",
    DeletedByUs = "DELETED_BY_US",
    DeletedByThem = "DELETED_BY_THEM",
    Unknown = "UNKNOWN",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: GitDiffSummary): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitDiffSummary): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
