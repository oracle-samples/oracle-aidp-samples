// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a Agent flow.
**/
export enum AgentFlowPrivilegeForManage {
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN",
    Use = "USE"
    
}

export namespace AgentFlowPrivilegeForManage {
    export function getJsonObj(obj: AgentFlowPrivilegeForManage): AgentFlowPrivilegeForManage {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AgentFlowPrivilegeForManage): AgentFlowPrivilegeForManage {
        return obj;
    }
}

