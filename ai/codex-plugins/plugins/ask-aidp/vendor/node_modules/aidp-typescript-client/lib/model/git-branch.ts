// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Git branch details.
*/
export interface GitBranch {
    /**
    * The name of the Git branch.
    */
    'branchName': string;
    /**
    * The Git repository URL corresponding to the branch.
    */
    'gitUrl': string;
    /**
    * Git error message.
    */
    'errorMessage'?: string;
    /**
    * Git STDOUT message.
    */
    'stdOut'?: string;
    /**
    * Git STDERR message.
    */
    'stdErr'?: string;
    /**
    * Git exit status. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'gitExitCode'?: number;

}

export namespace GitBranch {







    export function getJsonObj(obj: GitBranch): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitBranch): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
