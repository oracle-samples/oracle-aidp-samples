// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Status of the run.
**/
export enum DeploymentJobRunState {
    DeploymentJobRunStateUnspecified = "DEPLOYMENT_JOB_RUN_STATE_UNSPECIFIED",
    NoValidDeploymentJobFound = "NO_VALID_DEPLOYMENT_JOB_FOUND",
    Running = "RUNNING",
    Succeeded = "SUCCEEDED",
    Failed = "FAILED",
    Pending = "PENDING",
    Approval = "APPROVAL",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace DeploymentJobRunState {
    export function getJsonObj(obj: DeploymentJobRunState): DeploymentJobRunState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: DeploymentJobRunState): DeploymentJobRunState {
        return obj;
    }
}

