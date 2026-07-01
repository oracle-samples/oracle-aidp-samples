// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a Master Catalog. MASTER CATALOG ADMIN: This permission will allow the user to: - Grant/Revoke permissions - View all catalogs in the Master Catalog - Create Catalogs - DELETE Catalog CREATE CATALOG: This permission will enable users to add create a catalog (internal or external)
**/
export enum MasterCatalogPrivilege {
    CreateCatalog = "CREATE_CATALOG",
    Admin = "ADMIN",
    CreateShare = "CREATE_SHARE",
    CreateRecipient = "CREATE_RECIPIENT",
    CreateCredential = "CREATE_CREDENTIAL"
    
}

export namespace MasterCatalogPrivilege {
    export function getJsonObj(obj: MasterCatalogPrivilege): MasterCatalogPrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: MasterCatalogPrivilege): MasterCatalogPrivilege {
        return obj;
    }
}

