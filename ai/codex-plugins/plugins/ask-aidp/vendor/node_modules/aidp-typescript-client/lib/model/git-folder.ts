// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Represents a Git folder object.
*/
export interface GitFolder {
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
    'gitRepositoryUrl'?: string;
    /**
    * Short description about the git repository.
    */
    'description'?: string;
    /**
    * Git branch name that is cloned.
    */
    'branchName'?: string;
    /**
    * The metadata about the folder, like branchName.
    */
    'folderMetadata'?: { [key: string]: any; };

}

export namespace GitFolder {







    export function getJsonObj(obj: GitFolder): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitFolder): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
