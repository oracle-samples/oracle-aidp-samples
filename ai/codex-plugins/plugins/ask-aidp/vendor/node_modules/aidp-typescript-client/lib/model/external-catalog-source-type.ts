// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The source type of an external catalog.
**/
export enum ExternalCatalogSourceType {
    Adw = "ADW",
    Alh = "ALH",
    Kafka = "KAFKA",
    Atp = "ATP",
    Oracle = "ORACLE",
    Exadata = "EXADATA",
    OracleAnalytics = "ORACLE_ANALYTICS",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace ExternalCatalogSourceType {
    export function getJsonObj(obj: ExternalCatalogSourceType): ExternalCatalogSourceType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ExternalCatalogSourceType): ExternalCatalogSourceType {
        return obj;
    }
}

