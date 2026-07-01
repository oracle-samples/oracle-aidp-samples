// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* IAM user credential setting details.
*/
export interface IamUserCredentialUserSetting extends model.SettingData {
    /**
    * User OCID or User Name.
    */
    'userId': string;
    /**
    * Tenancy of the user.
    */
    'tenancy': string;
    /**
    * Region of the user.
    */
    'region': string;
    /**
    * Fingerprint.
    */
    'fingerprint': string;
    /**
    * Private API Key.
    */
    'privateApiKey': string;

   "type": string;
}

export namespace IamUserCredentialUserSetting {






    export function getJsonObj(obj: IamUserCredentialUserSetting, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SettingData.getJsonObj(obj) as IamUserCredentialUserSetting, ...{
            





        }};

        
        
        return jsonObj;
    }
    export const type = 'IAM_USER_CREDENTIAL';
    export function getDeserializedJsonObj(obj: IamUserCredentialUserSetting, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SettingData.getDeserializedJsonObj(obj) as IamUserCredentialUserSetting, ...{
            





         }};

        
        
        return jsonObj;
    }
}
