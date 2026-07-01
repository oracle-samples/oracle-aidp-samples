// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a Agent flow. Can be inherited privilege coming from object higher up in hierarchy.
**/
export enum AgentFlowPrivilege {
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN",
    Use = "USE"
    
}

export namespace AgentFlowPrivilege {
    export function getJsonObj(obj: AgentFlowPrivilege): AgentFlowPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AgentFlowPrivilege): AgentFlowPrivilege {
        return obj;
    }
}

