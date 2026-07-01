// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a Git folder in a workspace.
*/
export interface CreateGitFolderDetails {
    /**
    * The absolute path of the Git folder user wants to create.
    */
    'folderPath': string;
    /**
    * key corresponding to Git service provider in git provider table.
    */
    'gitProviderKey'?: string;
    /**
    * Git repository url used to clone.
    */
    'gitRepositoryUrl': string;
    /**
    * Git branch name that is cloned.
    */
    'branchName': string;
    /**
    * Credential key of the stored git credentials.
    */
    'credentialKey': string;
    /**
    * Short description about the git repository.
    */
    'description'?: string;

}

export namespace CreateGitFolderDetails {







    export function getJsonObj(obj: CreateGitFolderDetails): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateGitFolderDetails): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
