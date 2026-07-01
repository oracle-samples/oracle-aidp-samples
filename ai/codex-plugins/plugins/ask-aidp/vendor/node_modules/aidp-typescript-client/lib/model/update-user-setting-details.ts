// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Setting details to update for the user.
*/
export interface UpdateUserSettingDetails {
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

export namespace UpdateUserSettingDetails {




    export function getJsonObj(obj: UpdateUserSettingDetails): object {
        const jsonObj = {...obj, ...{
            


                'data': obj.data ?
                
                
                model.SettingData.getJsonObj(obj.data) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateUserSettingDetails): object {
        const jsonObj = {...obj, ...{
            


                    'data': obj.data ?
                
                
                model.SettingData.getDeserializedJsonObj(obj.data) : undefined,
         }};

        
        
        return jsonObj;
    }
}
