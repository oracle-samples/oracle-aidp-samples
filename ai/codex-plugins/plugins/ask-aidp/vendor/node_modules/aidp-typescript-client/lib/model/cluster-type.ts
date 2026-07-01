// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Cluster type. USER for clusters associated with workspace and created by users.
**/
export enum ClusterType {
    User = "USER",
    AgentFlowCompute = "AGENT_FLOW_COMPUTE",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ClusterType {
    export function getJsonObj(obj: ClusterType): ClusterType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ClusterType): ClusterType {
        return obj;
    }
}

