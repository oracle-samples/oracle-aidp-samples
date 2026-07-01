// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information for a commit.
*/
export interface HistorySummary {
    /**
    * Commit SHA.
    */
    'commitId': string;
    /**
    * First line of commit message (subject).
    */
    'message': string;
    /**
    * Commit author display name.
    */
    'authorName': string;
    /**
    * Commit author email (optional).
    */
    'authorEmail'?: string;
    /**
    * Commit authored time (or committed time if you prefer, but be consistent).
    */
    'timeCreated': Date;
    /**
    * True if commit has multiple parents.
    */
    'isMergeCommit'?: boolean;
    /**
    * Parent commit SHAs (empty for root commit). Present to explain merge commits.
    */
    'parents'?: Array<string>;

}

export namespace HistorySummary {








    export function getJsonObj(obj: HistorySummary): object {
        const jsonObj = {...obj, ...{
            







        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: HistorySummary): object {
        const jsonObj = {...obj, ...{
            







         }};

        
        
        return jsonObj;
    }
}
