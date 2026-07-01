// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of catalog migration operation
*/
export interface ExternalCatalogMigrationResult {
    /**
    * Total External catalog present Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'totalExternalCatalogs': number;
    /**
    * No of external catalogs with new design Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'newDesignExternalCatalogCount': number;
    /**
    * No of external catalogs migrated successfully
    */
    'migratedExternalCatalogs'?: Array<model.CatalogMigrationDetail>;
    /**
    * No of external catalogs migration failed
    */
    'failedMigrationExternalCatalogs'?: Array<model.CatalogMigrationDetail>;
    /**
    * Count of Catalog Failed with exception before migration Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'migrationFailedWithException'?: number;

}

export namespace ExternalCatalogMigrationResult {






    export function getJsonObj(obj: ExternalCatalogMigrationResult): object {
        const jsonObj = {...obj, ...{
            


                'migratedExternalCatalogs': obj.migratedExternalCatalogs ?
                
                obj.migratedExternalCatalogs.map((item)=>{return model.CatalogMigrationDetail.getJsonObj(item)})
                
                 : undefined,
                'failedMigrationExternalCatalogs': obj.failedMigrationExternalCatalogs ?
                
                obj.failedMigrationExternalCatalogs.map((item)=>{return model.CatalogMigrationDetail.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExternalCatalogMigrationResult): object {
        const jsonObj = {...obj, ...{
            


                    'migratedExternalCatalogs': obj.migratedExternalCatalogs ?
                
                obj.migratedExternalCatalogs.map((item)=>{return model.CatalogMigrationDetail.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'failedMigrationExternalCatalogs': obj.failedMigrationExternalCatalogs ?
                
                obj.failedMigrationExternalCatalogs.map((item)=>{return model.CatalogMigrationDetail.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
