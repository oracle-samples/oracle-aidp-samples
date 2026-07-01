// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Exadata external catalog connection property keys.
**/
export enum ExadataExternalCatalogConnectionPropertyKey {
    ExadataWalletContentBase64 = "EXADATA_WALLET_CONTENT_BASE64",
    ExadataWalletPassword = "EXADATA_WALLET_PASSWORD",
    ExadataUsername = "EXADATA_USERNAME",
    ExadataPassword = "EXADATA_PASSWORD",
    ExadataHost = "EXADATA_HOST",
    ExadataPort = "EXADATA_PORT",
    ExadataDatabaseName = "EXADATA_DATABASE_NAME",
    ExadataDatabaseSid = "EXADATA_DATABASE_SID",
    ExadataSslEnabled = "EXADATA_SSL_ENABLED",
    ExadataStagingTenancyOcid = "EXADATA_STAGING_TENANCY_OCID",
    ExadataStagingRegion = "EXADATA_STAGING_REGION",
    ExadataStagingNamespace = "EXADATA_STAGING_NAMESPACE",
    ExadataStagingBucket = "EXADATA_STAGING_BUCKET",
    ExadataStagingFilePrefix = "EXADATA_STAGING_FILE_PREFIX",
    WorkspaceKey = "WORKSPACE_KEY"
    
}

export namespace ExadataExternalCatalogConnectionPropertyKey {
    export function getJsonObj(obj: ExadataExternalCatalogConnectionPropertyKey): ExadataExternalCatalogConnectionPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ExadataExternalCatalogConnectionPropertyKey): ExadataExternalCatalogConnectionPropertyKey {
        return obj;
    }
}

