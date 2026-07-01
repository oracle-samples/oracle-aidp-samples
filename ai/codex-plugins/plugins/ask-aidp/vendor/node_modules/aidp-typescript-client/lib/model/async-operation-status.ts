// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The status of an async operation.
**/
export enum AsyncOperationStatus {
    InProgress = "IN_PROGRESS",
    Succeeded = "SUCCEEDED",
    Failed = "FAILED",
    Canceled = "CANCELED",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace AsyncOperationStatus {
    export function getJsonObj(obj: AsyncOperationStatus): AsyncOperationStatus {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AsyncOperationStatus): AsyncOperationStatus {
        return obj;
    }
}

