// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The location type of an external table.
**/
export enum ExternalTableLocationType {
    ObjectStorage = "OBJECT_STORAGE",
    Mount = "MOUNT",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ExternalTableLocationType {
    export function getJsonObj(obj: ExternalTableLocationType): ExternalTableLocationType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ExternalTableLocationType): ExternalTableLocationType {
        return obj;
    }
}

