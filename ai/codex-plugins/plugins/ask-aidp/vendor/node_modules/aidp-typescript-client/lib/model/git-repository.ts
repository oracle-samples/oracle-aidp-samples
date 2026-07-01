// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details about a Git repository.
*/
export interface GitRepository {
    /**
    * Unique key associated with repository.
    */
    'key': string;
    /**
    * The workspaceKey associated with the Git repository.
    */
    'workspaceKey': string;
    /**
    * The name of the Git branch.
    */
    'branchName': string;
    /**
    * The Git repository url corresponding to the branch.
    */
    'gitUrl': string;
    /**
    * The path of the current Git folder which has to be created for the new branch.
    */
    'gitFolderPath': string;
    /**
    * The credential setting key
    */
    'credentialKey'?: string;

}

export namespace GitRepository {







    export function getJsonObj(obj: GitRepository): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitRepository): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
