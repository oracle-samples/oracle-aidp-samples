// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The sort order to use, either ascending ({@code ASC}) or descending ({@code DESC}). The {@code displayName}
* sort order is case sensitive.
* 
**/
export enum SortOrder {
    Asc = "ASC",
    Desc = "DESC"
    
}

export namespace SortOrder {
    export function getJsonObj(obj: SortOrder): SortOrder {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SortOrder): SortOrder {
        return obj;
    }
}

