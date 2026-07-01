// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* OAuth admin setting details.
*/
export interface OAuthAdminSetting extends model.SettingData {
    /**
    * Issuer / authorization server base URL.
    */
    'identityProviderUrl': string;
    /**
    * URL to retrieve JKS keystore.
    */
    'retrieveJksUrl': string;

   "type": string;
}

export namespace OAuthAdminSetting {



    export function getJsonObj(obj: OAuthAdminSetting, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SettingData.getJsonObj(obj) as OAuthAdminSetting, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const type = 'OAUTH';
    export function getDeserializedJsonObj(obj: OAuthAdminSetting, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SettingData.getDeserializedJsonObj(obj) as OAuthAdminSetting, ...{
            


         }};

        
        
        return jsonObj;
    }
}
