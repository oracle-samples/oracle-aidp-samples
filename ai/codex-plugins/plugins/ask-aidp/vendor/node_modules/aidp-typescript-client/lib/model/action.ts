// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Action to enable or disable the logs.
**/
export enum Action {
    Enable = "ENABLE",
    Disable = "DISABLE"
    
}

export namespace Action {
    export function getJsonObj(obj: Action): Action {
        return obj;
    }
    export function getDeserializedJsonObj(obj: Action): Action {
        return obj;
    }
}

