// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Catalog property keys.
**/
export enum CatalogPropertyKey {
    BucketLocationCompartmentId = "BUCKET_LOCATION_COMPARTMENT_ID"
    
}

export namespace CatalogPropertyKey {
    export function getJsonObj(obj: CatalogPropertyKey): CatalogPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CatalogPropertyKey): CatalogPropertyKey {
        return obj;
    }
}

