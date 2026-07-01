// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a catalog.
*/
export interface CatalogSummary {
    /**
    * The AI Data Platform Workbench catalog key.
    */
    'key': string;
    /**
    * Catalog display name.
    */
    'displayName': string;
    /**
    * Short description of the catalog.
    */
    'description'?: string;
    /**
    * Type of catalog.
    */
    'catalogType'?: model.CatalogType;
    /**
    * Unique identifier for catalog.
    */
    'catalogGuid'?: string;
    /**
    * External catalog source type.
    */
    'sourceType'?: model.ExternalCatalogSourceType;
    /**
    * The current status of the catalog.
    */
    'lifecycleState'?: model.CatalogLifecycleState;
    /**
    * A message describing the current state in more detail. For example, it can be used to provide actionable information for a resource in Failed state.
    */
    'lifecycleStateDetails'?: string;
    /**
    * The date and time the AI Data Platform Workbench catalog was created.
    */
    'timeCreated': Date;
    /**
    * The date and time the AI Data Platform Workbench catalog was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The ID of the user that created the catalog.
    */
    'createdBy'?: string;
    /**
    * The ID of the last user to update the catalog.
    */
    'updatedBy'?: string;
    /**
    * The status for last refresh performed on catalog.
    */
    'lastRefreshStatus'?: model.CrawlerLastRefreshStatus;
    /**
    * The timestamp for last refresh performed on catalog.
    */
    'timeLastRefresh'?: Date;

}

export namespace CatalogSummary {















    export function getJsonObj(obj: CatalogSummary): object {
        const jsonObj = {...obj, ...{
            














        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CatalogSummary): object {
        const jsonObj = {...obj, ...{
            














         }};

        
        
        return jsonObj;
    }
}
