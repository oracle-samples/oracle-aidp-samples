// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * ALH external catalog connection property keys.
**/
export enum AlhExternalCatalogConnectionPropertyKey {
    AlhWalletContentBase64 = "ALH_WALLET_CONTENT_BASE64",
    AlhWalletPassword = "ALH_WALLET_PASSWORD",
    AlhUsername = "ALH_USERNAME",
    AlhPassword = "ALH_PASSWORD",
    AlhTnsAlias = "ALH_TNS_ALIAS",
    AlhStagingTenancyOcid = "ALH_STAGING_TENANCY_OCID",
    AlhStagingRegion = "ALH_STAGING_REGION",
    AlhStagingNamespace = "ALH_STAGING_NAMESPACE",
    AlhStagingBucket = "ALH_STAGING_BUCKET",
    AlhStagingFilePrefix = "ALH_STAGING_FILE_PREFIX"
    
}

export namespace AlhExternalCatalogConnectionPropertyKey {
    export function getJsonObj(obj: AlhExternalCatalogConnectionPropertyKey): AlhExternalCatalogConnectionPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AlhExternalCatalogConnectionPropertyKey): AlhExternalCatalogConnectionPropertyKey {
        return obj;
    }
}

