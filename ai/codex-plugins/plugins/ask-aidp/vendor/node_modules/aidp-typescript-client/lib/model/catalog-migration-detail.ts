// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a catalog.
*/
export interface CatalogMigrationDetail {
    /**
    * catalog key
    */
    'key': string;
    /**
    * catalog Name
    */
    'catalogName': string;
    /**
    * catalog type
    */
    'catalogType': string;
    /**
    * migration result status
    */
    'status': CatalogMigrationDetail.Status;
    /**
    * Failure msg if failed else null
    */
    'failureMsg'?: string;

}

export namespace CatalogMigrationDetail {




    export enum Status {
    
    Success = "SUCCESS",
    Failed = "FAILED"

}



    export function getJsonObj(obj: CatalogMigrationDetail): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CatalogMigrationDetail): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
