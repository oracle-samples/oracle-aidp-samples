// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information required to create a new Git branch.
*/
export interface CreateGitBranchDetails {
    /**
    * Git branch name that is cloned.
    */
    'gitBranchName': string;
    /**
    * The path of the current Git folder which has to be created for the new branch.
    */
    'gitFolderPath'?: string;

}

export namespace CreateGitBranchDetails {



    export function getJsonObj(obj: CreateGitBranchDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateGitBranchDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
