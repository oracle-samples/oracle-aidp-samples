// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Git branch object summary.
*/
export interface GitBranchSummary {
    /**
    * The name of the Git branch.
    */
    'branchName': string;
    /**
    * Fully qualified branch path.
    */
    'branchPath': string;

}

export namespace GitBranchSummary {



    export function getJsonObj(obj: GitBranchSummary): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitBranchSummary): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
