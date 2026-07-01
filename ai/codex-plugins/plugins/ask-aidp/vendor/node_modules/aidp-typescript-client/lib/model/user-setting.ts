// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* User Setting
*/
export interface UserSetting {
    /**
    * The unique identifier for the setting.
    */
    'key': string;
    /**
    * A user-friendly name for the setting.
    */
    'name': string;
    /**
    * Indicates whether this setting is the default.
    */
    'isDefault': boolean;
    'data'?: model.GitAccountUserSetting| model.IamUserCredentialUserSetting| model.OAuthAdminSetting;

}

export namespace UserSetting {





    export function getJsonObj(obj: UserSetting): object {
        const jsonObj = {...obj, ...{
            



                'data': obj.data ?
                
                
                model.SettingData.getJsonObj(obj.data) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UserSetting): object {
        const jsonObj = {...obj, ...{
            



                    'data': obj.data ?
                
                
                model.SettingData.getDeserializedJsonObj(obj.data) : undefined,
         }};

        
        
        return jsonObj;
    }
}
