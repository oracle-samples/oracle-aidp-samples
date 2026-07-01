// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to attach a Master Catalog to a Cluster
*/
export interface AttachMasterCatalogToClusterDetails {
    /**
    * The key of the Cluster to attach Master Catalog
    */
    'clusterKey': string;

}

export namespace AttachMasterCatalogToClusterDetails {


    export function getJsonObj(obj: AttachMasterCatalogToClusterDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AttachMasterCatalogToClusterDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
