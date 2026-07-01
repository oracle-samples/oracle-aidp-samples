// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Action to take when policy is violated
**/
export enum PolicyAction {
    Block = "BLOCK",
    Inform = "INFORM",
    Mask = "MASK"
    
}

export namespace PolicyAction {
    export function getJsonObj(obj: PolicyAction): PolicyAction {
        return obj;
    }
    export function getDeserializedJsonObj(obj: PolicyAction): PolicyAction {
        return obj;
    }
}

