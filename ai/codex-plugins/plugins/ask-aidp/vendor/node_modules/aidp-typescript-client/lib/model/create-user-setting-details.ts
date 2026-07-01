// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Settings details for the new user.
*/
export interface CreateUserSettingDetails {
    /**
    * A user-friendly name for the setting.
    */
    'name': string;
    /**
    * Indicates whether this setting is the default.
    */
    'isDefault': boolean;
    'data': model.GitAccountUserSetting| model.IamUserCredentialUserSetting| model.OAuthAdminSetting;

}

export namespace CreateUserSettingDetails {




    export function getJsonObj(obj: CreateUserSettingDetails): object {
        const jsonObj = {...obj, ...{
            


                'data': obj.data ?
                
                
                model.SettingData.getJsonObj(obj.data) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateUserSettingDetails): object {
        const jsonObj = {...obj, ...{
            


                    'data': obj.data ?
                
                
                model.SettingData.getDeserializedJsonObj(obj.data) : undefined,
         }};

        
        
        return jsonObj;
    }
}
