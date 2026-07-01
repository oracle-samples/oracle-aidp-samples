// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Status of the run.
**/
export enum ExperimentRunStatus {
    Running = "RUNNING",
    Scheduled = "SCHEDULED",
    Finished = "FINISHED",
    Failed = "FAILED",
    Killed = "KILLED",
    InternalError = "INTERNAL_ERROR",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ExperimentRunStatus {
    export function getJsonObj(obj: ExperimentRunStatus): ExperimentRunStatus {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ExperimentRunStatus): ExperimentRunStatus {
        return obj;
    }
}

