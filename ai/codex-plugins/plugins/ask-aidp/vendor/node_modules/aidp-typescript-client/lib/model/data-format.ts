// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The table data format type.
**/
export enum DataFormat {
    Avro = "AVRO",
    Orc = "ORC",
    Parquet = "PARQUET",
    Textfile = "TEXTFILE",
    Json = "JSON",
    Csv = "CSV",
    Delta = "DELTA",
    Iceberg = "ICEBERG",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace DataFormat {
    export function getJsonObj(obj: DataFormat): DataFormat {
        return obj;
    }
    export function getDeserializedJsonObj(obj: DataFormat): DataFormat {
        return obj;
    }
}

