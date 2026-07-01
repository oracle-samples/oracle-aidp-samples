// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Oracle Database external catalog connection property keys.
**/
export enum OracleExternalCatalogConnectionPropertyKey {
    OracleWalletContentBase64 = "ORACLE_WALLET_CONTENT_BASE64",
    OracleWalletPassword = "ORACLE_WALLET_PASSWORD",
    OracleUsername = "ORACLE_USERNAME",
    OraclePassword = "ORACLE_PASSWORD",
    OracleHost = "ORACLE_HOST",
    OraclePort = "ORACLE_PORT",
    OracleDatabaseName = "ORACLE_DATABASE_NAME",
    OracleDatabaseSid = "ORACLE_DATABASE_SID",
    OracleSslEnabled = "ORACLE_SSL_ENABLED",
    OracleStagingTenancyOcid = "ORACLE_STAGING_TENANCY_OCID",
    OracleStagingRegion = "ORACLE_STAGING_REGION",
    OracleStagingNamespace = "ORACLE_STAGING_NAMESPACE",
    OracleStagingBucket = "ORACLE_STAGING_BUCKET",
    OracleStagingFilePrefix = "ORACLE_STAGING_FILE_PREFIX"
    
}

export namespace OracleExternalCatalogConnectionPropertyKey {
    export function getJsonObj(obj: OracleExternalCatalogConnectionPropertyKey): OracleExternalCatalogConnectionPropertyKey {
        return obj;
    }
    export function getDeserializedJsonObj(obj: OracleExternalCatalogConnectionPropertyKey): OracleExternalCatalogConnectionPropertyKey {
        return obj;
    }
}

