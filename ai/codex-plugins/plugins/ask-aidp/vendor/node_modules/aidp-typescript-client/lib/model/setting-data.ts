// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Setting Data
*/
export interface SettingData {

   "type": string;
}

export namespace SettingData {

    export function getJsonObj(obj: SettingData): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "GIT_ACCOUNT":
                    return model.GitAccountUserSetting.getJsonObj(<model.GitAccountUserSetting>(<object>jsonObj), true);
                case "IAM_USER_CREDENTIAL":
                    return model.IamUserCredentialUserSetting.getJsonObj(<model.IamUserCredentialUserSetting>(<object>jsonObj), true);
                case "OAUTH":
                    return model.OAuthAdminSetting.getJsonObj(<model.OAuthAdminSetting>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SettingData): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "GIT_ACCOUNT":
                    return model.GitAccountUserSetting.getDeserializedJsonObj(<model.GitAccountUserSetting>(<object>jsonObj), true);
                case "IAM_USER_CREDENTIAL":
                    return model.IamUserCredentialUserSetting.getDeserializedJsonObj(<model.IamUserCredentialUserSetting>(<object>jsonObj), true);
                case "OAUTH":
                    return model.OAuthAdminSetting.getDeserializedJsonObj(<model.OAuthAdminSetting>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
