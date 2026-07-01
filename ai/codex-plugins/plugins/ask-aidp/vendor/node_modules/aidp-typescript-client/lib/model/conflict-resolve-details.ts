// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of files to checkout side or mark as resolved.
*/
export interface ConflictResolveDetails {
    /**
    * List of file paths to checkout side or mark as resolved.
    */
    'files': Array<string>;
    /**
    * Action for resolving conflict.
    */
    'action': ConflictResolveDetails.Action;

}

export namespace ConflictResolveDetails {


    export enum Action {
    
    Local = "LOCAL",
    Remote = "REMOTE",
    MarkResolved = "MARK_RESOLVED"

}


    export function getJsonObj(obj: ConflictResolveDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ConflictResolveDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
