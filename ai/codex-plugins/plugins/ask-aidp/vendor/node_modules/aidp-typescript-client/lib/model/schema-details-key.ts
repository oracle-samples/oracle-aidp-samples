// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Schema details key. This can be referred to when the related catalog is an external (Non-ADW) catalog.
**/
export enum SchemaDetailsKey {
    Partitions = "PARTITIONS",
    ReplicationFactor = "REPLICATION_FACTOR",
    BootstrapServers = "BOOTSTRAP_SERVERS",
    PartitionsDetails = "PARTITIONS_DETAILS"
    
}

export namespace SchemaDetailsKey {
    export function getJsonObj(obj: SchemaDetailsKey): SchemaDetailsKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: SchemaDetailsKey): SchemaDetailsKey {
        return obj;
    }
}

