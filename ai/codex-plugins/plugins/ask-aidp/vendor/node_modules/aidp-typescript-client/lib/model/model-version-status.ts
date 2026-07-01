// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Status of the run.
**/
export enum ModelVersionStatus {
    PendingRegistration = "PENDING_REGISTRATION",
    FailedRegistration = "FAILED_REGISTRATION",
    Ready = "READY",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ModelVersionStatus {
    export function getJsonObj(obj: ModelVersionStatus): ModelVersionStatus {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ModelVersionStatus): ModelVersionStatus {
        return obj;
    }
}

