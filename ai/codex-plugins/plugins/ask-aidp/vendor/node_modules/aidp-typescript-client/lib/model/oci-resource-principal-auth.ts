// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Auth configuration while using AIDP resource principal for auth
*/
export interface OciResourcePrincipalAuth extends model.Auth {

   "authType": string;
}

export namespace OciResourcePrincipalAuth {

    export function getJsonObj(obj: OciResourcePrincipalAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getJsonObj(obj) as OciResourcePrincipalAuth, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const authType = 'OCI_RESOURCE_PRINCIPAL';
    export function getDeserializedJsonObj(obj: OciResourcePrincipalAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getDeserializedJsonObj(obj) as OciResourcePrincipalAuth, ...{
            
         }};

        
        
        return jsonObj;
    }
}
