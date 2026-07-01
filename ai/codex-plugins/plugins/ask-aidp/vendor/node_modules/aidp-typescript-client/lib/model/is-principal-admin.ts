// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response indicating whether the principal has admin privileges.
*/
export interface IsPrincipalAdmin {
    /**
    * True if the principal has admin privileges, otherwise false.
    */
    'isAdmin': boolean;

}

export namespace IsPrincipalAdmin {


    export function getJsonObj(obj: IsPrincipalAdmin): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: IsPrincipalAdmin): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
