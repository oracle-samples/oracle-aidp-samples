// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Accepted values of sort field
**/
export enum SortFieldEnum {
    TimeUpdated = "TIME_UPDATED",
    TimeCreated = "TIME_CREATED",
    Relevancy = "RELEVANCY",
    Type = "TYPE"
    
}

export namespace SortFieldEnum {
    export function getJsonObj(obj: SortFieldEnum): SortFieldEnum {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SortFieldEnum): SortFieldEnum {
        return obj;
    }
}

