// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The action for an operation to grant/revoke consumption access on a share to/from a recipient.
**/
export enum ShareAccessAction {
    Grant = "GRANT",
    Revoke = "REVOKE"
    
}

export namespace ShareAccessAction {
    export function getJsonObj(obj: ShareAccessAction): ShareAccessAction {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ShareAccessAction): ShareAccessAction {
        return obj;
    }
}

