// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response indicating whether the principal has AIDP admin RBAC privileges.
*/
export interface IsPrincipalAidpAdmin {
    /**
    * True if the principal has AIDP admin RBAC privileges , otherwise false.
    */
    'isAidpAdmin': boolean;

}

export namespace IsPrincipalAidpAdmin {


    export function getJsonObj(obj: IsPrincipalAidpAdmin): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: IsPrincipalAidpAdmin): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
