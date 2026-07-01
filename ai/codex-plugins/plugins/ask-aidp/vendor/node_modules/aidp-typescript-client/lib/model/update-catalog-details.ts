// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details to update in a catalog.
*/
export interface UpdateCatalogDetails {
    /**
    * Catalog display name.
    */
    'displayName'?: string;
    /**
    * Short description of the catalog.
    */
    'description'?: string;
    'connectionDetails'?: model.UpdateConnectionDetails;
    /**
    * Key-value pair used to provide catalog properties like the subCompartment OCID where the buckets need to reside.
    */
    'properties'?: { [key: string]: string; };

}

export namespace UpdateCatalogDetails {





    export function getJsonObj(obj: UpdateCatalogDetails): object {
        const jsonObj = {...obj, ...{
            


                'connectionDetails': obj.connectionDetails ?
                
                
                model.UpdateConnectionDetails.getJsonObj(obj.connectionDetails) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateCatalogDetails): object {
        const jsonObj = {...obj, ...{
            


                    'connectionDetails': obj.connectionDetails ?
                
                
                model.UpdateConnectionDetails.getDeserializedJsonObj(obj.connectionDetails) : undefined,

         }};

        
        
        return jsonObj;
    }
}
