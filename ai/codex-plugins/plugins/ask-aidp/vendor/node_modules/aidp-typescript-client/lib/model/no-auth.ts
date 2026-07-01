// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Auth configuration while using no auth mode
*/
export interface NoAuth extends model.Auth {

   "authType": string;
}

export namespace NoAuth {

    export function getJsonObj(obj: NoAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getJsonObj(obj) as NoAuth, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const authType = 'NO_AUTH';
    export function getDeserializedJsonObj(obj: NoAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getDeserializedJsonObj(obj) as NoAuth, ...{
            
         }};

        
        
        return jsonObj;
    }
}
