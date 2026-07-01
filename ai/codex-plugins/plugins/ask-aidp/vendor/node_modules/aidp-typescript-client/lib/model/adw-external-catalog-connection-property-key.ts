// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * ADW external catalog connection property keys.
**/
export enum AdwExternalCatalogConnectionPropertyKey {
    AdwWalletContentBase64 = "ADW_WALLET_CONTENT_BASE64",
    AdwWalletPassword = "ADW_WALLET_PASSWORD",
    AdwUsername = "ADW_USERNAME",
    AdwPassword = "ADW_PASSWORD",
    AdwTnsAlias = "ADW_TNS_ALIAS",
    AdwStagingTenancyOcid = "ADW_STAGING_TENANCY_OCID",
    AdwStagingRegion = "ADW_STAGING_REGION",
    AdwStagingNamespace = "ADW_STAGING_NAMESPACE",
    AdwStagingBucket = "ADW_STAGING_BUCKET",
    AdwStagingFilePrefix = "ADW_STAGING_FILE_PREFIX"
    
}

export namespace AdwExternalCatalogConnectionPropertyKey {
    export function getJsonObj(obj: AdwExternalCatalogConnectionPropertyKey): AdwExternalCatalogConnectionPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AdwExternalCatalogConnectionPropertyKey): AdwExternalCatalogConnectionPropertyKey {
        return obj;
    }
}

