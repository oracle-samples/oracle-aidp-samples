// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Auth configuration while using bearer token
*/
export interface BearerTokenAuth extends model.Auth {
    /**
    * The bearer token used for auth
    */
    'token'?: string;

   "authType": string;
}

export namespace BearerTokenAuth {


    export function getJsonObj(obj: BearerTokenAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getJsonObj(obj) as BearerTokenAuth, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const authType = 'BEARER_TOKEN';
    export function getDeserializedJsonObj(obj: BearerTokenAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getDeserializedJsonObj(obj) as BearerTokenAuth, ...{
            

         }};

        
        
        return jsonObj;
    }
}
