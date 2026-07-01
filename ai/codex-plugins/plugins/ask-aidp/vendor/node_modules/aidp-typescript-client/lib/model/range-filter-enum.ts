// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Accepted values of range filters
**/
export enum RangeFilterEnum {
    TimeUpdated = "TIME_UPDATED",
    TimeCreated = "TIME_CREATED"
    
}

export namespace RangeFilterEnum {
    export function getJsonObj(obj: RangeFilterEnum): RangeFilterEnum {
        return obj;
    }
    export function getDeserializedJsonObj(obj: RangeFilterEnum): RangeFilterEnum {
        return obj;
    }
}

