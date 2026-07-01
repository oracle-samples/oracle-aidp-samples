// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Possible types of search object
**/
export enum AggregationEnum {
    Type = "TYPE",
    Owner = "OWNER",
    CreatedBy = "CREATED_BY",
    Catalog = "CATALOG",
    Schema = "SCHEMA",
    Workspace = "WORKSPACE"
    
}

export namespace AggregationEnum {
    export function getJsonObj(obj: AggregationEnum): AggregationEnum {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AggregationEnum): AggregationEnum {
        return obj;
    }
}

