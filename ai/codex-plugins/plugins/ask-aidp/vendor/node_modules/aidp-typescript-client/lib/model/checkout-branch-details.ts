// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information required to checkout a new Git branch.
*/
export interface CheckoutBranchDetails {
    /**
    * Git branch name that is cloned.
    */
    'branchName': string;
    /**
    * The path of the Git folder in the context.
    */
    'gitFolderPath': string;

}

export namespace CheckoutBranchDetails {



    export function getJsonObj(obj: CheckoutBranchDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CheckoutBranchDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
