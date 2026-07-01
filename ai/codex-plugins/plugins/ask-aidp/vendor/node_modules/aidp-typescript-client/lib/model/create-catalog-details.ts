// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The required details for creating catalog.
*/
export interface CreateCatalogDetails {
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
    * External catalog source type.
    */
    'sourceType'?: model.ExternalCatalogSourceType;
    /**
    * Key-value pair used to provide catalog properties like the subCompartment OCID where the buckets need to reside.
    */
    'properties'?: { [key: string]: string; };
    'connectionDetails'?: model.CreateConnectionDetails;

}

export namespace CreateCatalogDetails {







    export function getJsonObj(obj: CreateCatalogDetails): object {
        const jsonObj = {...obj, ...{
            





                'connectionDetails': obj.connectionDetails ?
                
                
                model.CreateConnectionDetails.getJsonObj(obj.connectionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateCatalogDetails): object {
        const jsonObj = {...obj, ...{
            





                    'connectionDetails': obj.connectionDetails ?
                
                
                model.CreateConnectionDetails.getDeserializedJsonObj(obj.connectionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
