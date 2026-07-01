// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The state of the Master Catalog
**/
export enum MasterCatalogLifecycleState {
    Active = "ACTIVE",
    Creating = "CREATING",
    Deleting = "DELETING"
    
}

export namespace MasterCatalogLifecycleState {
    export function getJsonObj(obj: MasterCatalogLifecycleState): MasterCatalogLifecycleState {
        return obj;
    }
    export function getDeserializedJsonObj(obj: MasterCatalogLifecycleState): MasterCatalogLifecycleState {
        return obj;
    }
}

