// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Auth configuration while using oauth token
*/
export interface OAuth extends model.Auth {
    /**
    * The clientId of the confidential app for auth
    */
    'clientId'?: string;
    /**
    * The secret of the confidential app for auth
    */
    'clientSecret'?: string;
    /**
    * The endpoint for the issuer idp
    */
    'issuerIdpEndpoint'?: string;
    /**
    * The list of scopes for oauth
    */
    'scopes'?: Array<string>;

   "authType": string;
}

export namespace OAuth {





    export function getJsonObj(obj: OAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getJsonObj(obj) as OAuth, ...{
            




        }};

        
        
        return jsonObj;
    }
    export const authType = 'OAUTH';
    export function getDeserializedJsonObj(obj: OAuth, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Auth.getDeserializedJsonObj(obj) as OAuth, ...{
            




         }};

        
        
        return jsonObj;
    }
}
