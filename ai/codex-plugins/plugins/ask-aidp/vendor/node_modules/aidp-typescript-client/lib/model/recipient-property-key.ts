// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Recipient property keys.
**/
export enum RecipientPropertyKey {
    ActivationLinkEmail = "ACTIVATION_LINK_EMAIL",
    DeltaShareEndpoint = "DELTA_SHARE_ENDPOINT",
    ActivationLink = "ACTIVATION_LINK",
    ActivationTokenKey = "ACTIVATION_TOKEN_KEY",
    ActivationTokenExpiryTime = "ACTIVATION_TOKEN_EXPIRY_TIME",
    BearerTokenKey = "BEARER_TOKEN_KEY",
    BearerTokenExpiryTime = "BEARER_TOKEN_EXPIRY_TIME"
    
}

export namespace RecipientPropertyKey {
    export function getJsonObj(obj: RecipientPropertyKey): RecipientPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: RecipientPropertyKey): RecipientPropertyKey {
        return obj;
    }
}

