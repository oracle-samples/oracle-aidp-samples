// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * ATP external catalog connection property keys.
**/
export enum AtpExternalCatalogConnectionPropertyKey {
    AtpWalletContentBase64 = "ATP_WALLET_CONTENT_BASE64",
    AtpWalletPassword = "ATP_WALLET_PASSWORD",
    AtpUsername = "ATP_USERNAME",
    AtpPassword = "ATP_PASSWORD",
    AtpTnsAlias = "ATP_TNS_ALIAS",
    AtpStagingTenancyOcid = "ATP_STAGING_TENANCY_OCID",
    AtpStagingRegion = "ATP_STAGING_REGION",
    AtpStagingNamespace = "ATP_STAGING_NAMESPACE",
    AtpStagingBucket = "ATP_STAGING_BUCKET",
    AtpStagingFilePrefix = "ATP_STAGING_FILE_PREFIX"
    
}

export namespace AtpExternalCatalogConnectionPropertyKey {
    export function getJsonObj(obj: AtpExternalCatalogConnectionPropertyKey): AtpExternalCatalogConnectionPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AtpExternalCatalogConnectionPropertyKey): AtpExternalCatalogConnectionPropertyKey {
        return obj;
    }
}

