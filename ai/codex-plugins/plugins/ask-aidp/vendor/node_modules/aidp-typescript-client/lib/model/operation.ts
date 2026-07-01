// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Type of operation
**/
export enum Operation {
    Create = "CREATE",
    Update = "UPDATE",
    Delete = "DELETE",
    View = "VIEW",
    Grant = "GRANT",
    Revoke = "REVOKE",
    Attach = "ATTACH",
    Detach = "DETACH",
    ViewLogs = "VIEW_LOGS",
    Rename = "RENAME",
    Terminate = "TERMINATE",
    Move = "MOVE",
    Execute = "EXECUTE",
    ManageAccess = "MANAGE_ACCESS",
    Query = "QUERY",
    Manage = "MANAGE",
    Read = "READ",
    Write = "WRITE",
    Start = "START",
    Stop = "STOP",
    Copy = "COPY",
    Deploy = "DEPLOY",
    Undeploy = "UNDEPLOY",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace Operation {
    export function getJsonObj(obj: Operation): Operation {
        return obj;
    }
    export function getDeserializedJsonObj(obj: Operation): Operation {
        return obj;
    }
}

