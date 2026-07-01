// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* OAuth configuration for agent flow
*/
export interface OAuthConfiguration {
    /**
    * Issuer Claim of OAuthConfiguration
    */
    'issuerClaim'?: string;
    /**
    * List of Audience Claim of OAuthConfiguration
    */
    'audienceClaim'?: Array<string>;
    /**
    * JWKS URI of OAuthConfiguration
    */
    'jwksUri'?: string;

}

export namespace OAuthConfiguration {




    export function getJsonObj(obj: OAuthConfiguration): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: OAuthConfiguration): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
