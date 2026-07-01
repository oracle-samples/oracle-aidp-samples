// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Accepted values of list filters
**/
export enum ListFilterEnum {
    Type = "TYPE",
    Owner = "OWNER",
    CreatedBy = "CREATED_BY",
    Catalog = "CATALOG",
    Schema = "SCHEMA",
    Workspace = "WORKSPACE",
    Job = "JOB",
    Cluster = "CLUSTER"
    
}

export namespace ListFilterEnum {
    export function getJsonObj(obj: ListFilterEnum): ListFilterEnum {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ListFilterEnum): ListFilterEnum {
        return obj;
    }
}

