// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary of admin settings.
*/
export interface AdminSettingSummary {
    /**
    * The unique identifier for the setting.
    */
    'key': string;
    /**
    * A user-friendly name for the setting.
    */
    'name': string;
    /**
    * Setting data type discriminator
    */
    'type': model.SettingType;
    /**
    * Indicates whether this setting is the default.
    */
    'isDefault': boolean;
    /**
    * The date and time when the setting was created.
    */
    'timeCreated': Date;
    /**
    * The date and time when the setting was most recently updated.
    */
    'timeUpdated': Date;
    /**
    * A generic property bag associated with the setting resource.
    */
    'properties'?: { [key: string]: any; };

}

export namespace AdminSettingSummary {








    export function getJsonObj(obj: AdminSettingSummary): object {
        const jsonObj = {...obj, ...{
            







        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AdminSettingSummary): object {
        const jsonObj = {...obj, ...{
            







         }};

        
        
        return jsonObj;
    }
}
