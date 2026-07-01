// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Oracle Analytics external catalog connection property keys
**/
export enum OacExternalCatalogConnectionPropertyKey {
    OacEndpointUrl = "OAC_ENDPOINT_URL",
    OacIdcsEndpointUrl = "OAC_IDCS_ENDPOINT_URL",
    OacIdcsClientId = "OAC_IDCS_CLIENT_ID",
    OacIdcsClientScope = "OAC_IDCS_CLIENT_SCOPE",
    OacIdcsCertificate = "OAC_IDCS_CERTIFICATE",
    OacIdcsPrivateKey = "OAC_IDCS_PRIVATE_KEY"
    
}

export namespace OacExternalCatalogConnectionPropertyKey {
    export function getJsonObj(obj: OacExternalCatalogConnectionPropertyKey): OacExternalCatalogConnectionPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: OacExternalCatalogConnectionPropertyKey): OacExternalCatalogConnectionPropertyKey {
        return obj;
    }
}

