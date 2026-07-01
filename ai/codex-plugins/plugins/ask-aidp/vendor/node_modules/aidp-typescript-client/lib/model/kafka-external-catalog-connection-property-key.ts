// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Kafka external catalog connection property keys.
**/
export enum KafkaExternalCatalogConnectionPropertyKey {
    BootstrapServers = "BOOTSTRAP_SERVERS",
    Username = "USERNAME",
    Password = "PASSWORD",
    EnableSsl = "ENABLE_SSL",
    SaslMode = "SASL_MODE"
    
}

export namespace KafkaExternalCatalogConnectionPropertyKey {
    export function getJsonObj(obj: KafkaExternalCatalogConnectionPropertyKey): KafkaExternalCatalogConnectionPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: KafkaExternalCatalogConnectionPropertyKey): KafkaExternalCatalogConnectionPropertyKey {
        return obj;
    }
}

