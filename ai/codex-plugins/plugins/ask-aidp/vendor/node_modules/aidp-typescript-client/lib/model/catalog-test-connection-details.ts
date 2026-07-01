// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information needed to test connection to an external catalog.
*/
export interface CatalogTestConnectionDetails {
    /**
    * The AI Data Platform Workbench catalog key.
    */
    'key'?: string;
    /**
    * External catalog source type.
    */
    'sourceType'?: model.ExternalCatalogSourceType;
    'connectionDetails'?: model.CatalogConnectionDetails;

}

export namespace CatalogTestConnectionDetails {




    export function getJsonObj(obj: CatalogTestConnectionDetails): object {
        const jsonObj = {...obj, ...{
            


                'connectionDetails': obj.connectionDetails ?
                
                
                model.CatalogConnectionDetails.getJsonObj(obj.connectionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CatalogTestConnectionDetails): object {
        const jsonObj = {...obj, ...{
            


                    'connectionDetails': obj.connectionDetails ?
                
                
                model.CatalogConnectionDetails.getDeserializedJsonObj(obj.connectionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
