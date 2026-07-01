// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Update a Git repository with the provided details.
*/
export interface UpdateGitRepositoryDetails {
    /**
    * The path of the current Git folder which has to be created for the new branch.
    */
    'gitFolderPath'?: string;
    /**
    * The Git repository URL corresponding to the branch.
    */
    'gitUrl'?: string;
    /**
    * Updated PAT credential key.
    */
    'credentialKey'?: string;

}

export namespace UpdateGitRepositoryDetails {




    export function getJsonObj(obj: UpdateGitRepositoryDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateGitRepositoryDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
