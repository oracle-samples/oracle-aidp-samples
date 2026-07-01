// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to uninstall a library from a cluster.
*/
export interface UninstallClusterLibraryDetails extends model.ClusterLibraryDetails {
    /**
    * Name of the library to uninstall.
    */
    'name'?: string;

   "operation": string;
}

export namespace UninstallClusterLibraryDetails {


    export function getJsonObj(obj: UninstallClusterLibraryDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterLibraryDetails.getJsonObj(obj) as UninstallClusterLibraryDetails, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const operation = 'UNINSTALL';
    export function getDeserializedJsonObj(obj: UninstallClusterLibraryDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterLibraryDetails.getDeserializedJsonObj(obj) as UninstallClusterLibraryDetails, ...{
            

         }};

        
        
        return jsonObj;
    }
}
