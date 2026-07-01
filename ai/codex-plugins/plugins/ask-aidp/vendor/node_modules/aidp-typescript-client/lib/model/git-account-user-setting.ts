// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Git account setting details.
*/
export interface GitAccountUserSetting extends model.SettingData {
    /**
    * The name of the Git provider.
    */
    'providerName': model.GitAccountProviderName;
    /**
    * The type of Git account entity.
    */
    'entityType': GitAccountUserSetting.EntityType;
    /**
    * The username for the Git account.
    */
    'username'?: string;
    /**
    * The personal access token for the Git account.
    */
    'personalAccessToken'?: string;

   "type": string;
}

export namespace GitAccountUserSetting {


    export enum EntityType {
    
    PersonalAccessToken = "PERSONAL_ACCESS_TOKEN",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}




    export function getJsonObj(obj: GitAccountUserSetting, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SettingData.getJsonObj(obj) as GitAccountUserSetting, ...{
            




        }};

        
        
        return jsonObj;
    }
    export const type = 'GIT_ACCOUNT';
    export function getDeserializedJsonObj(obj: GitAccountUserSetting, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SettingData.getDeserializedJsonObj(obj) as GitAccountUserSetting, ...{
            




         }};

        
        
        return jsonObj;
    }
}
