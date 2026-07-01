// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Status of the job.
**/
export enum DeploymentJobState {
    DeploymentJobConnectionStateUnspecified = "DEPLOYMENT_JOB_CONNECTION_STATE_UNSPECIFIED",
    NotSetUp = "NOT_SET_UP",
    Connected = "CONNECTED",
    NotFound = "NOT_FOUND",
    RequiredParametersChanged = "REQUIRED_PARAMETERS_CHANGED",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace DeploymentJobState {
    export function getJsonObj(obj: DeploymentJobState): DeploymentJobState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: DeploymentJobState): DeploymentJobState {
        return obj;
    }
}

