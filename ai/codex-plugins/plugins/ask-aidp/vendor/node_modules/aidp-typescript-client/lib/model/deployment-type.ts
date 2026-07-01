// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of an Agent Flow Deployment.
**/
export enum DeploymentType {
    Test = "TEST",
    Prod = "PROD",
    Code = "CODE"
    
}

export namespace DeploymentType {
    export function getJsonObj(obj: DeploymentType): DeploymentType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: DeploymentType): DeploymentType {
        return obj;
    }
}

