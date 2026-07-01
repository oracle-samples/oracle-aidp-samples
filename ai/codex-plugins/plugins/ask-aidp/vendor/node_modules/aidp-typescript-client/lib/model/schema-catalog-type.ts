// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The type of catalog for the schema.
**/
export enum SchemaCatalogType {
    Adw = "ADW",
    Alh = "ALH",
    Standard = "STANDARD",
    KafkaTopic = "KAFKA_TOPIC",
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

export namespace SchemaCatalogType {
    export function getJsonObj(obj: SchemaCatalogType): SchemaCatalogType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SchemaCatalogType): SchemaCatalogType {
        return obj;
    }
}

