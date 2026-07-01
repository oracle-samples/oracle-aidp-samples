// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The type of setting data.
**/
export enum SettingType {
    IamUserCredential = "IAM_USER_CREDENTIAL",
    GitAccount = "GIT_ACCOUNT",
    Oauth = "OAUTH",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace SettingType {
    export function getJsonObj(obj: SettingType): SettingType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SettingType): SettingType {
        return obj;
    }
}

