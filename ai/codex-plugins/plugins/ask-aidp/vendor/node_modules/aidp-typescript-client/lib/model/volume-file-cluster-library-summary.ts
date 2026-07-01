// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of a volume file installed as a library on a cluster.
*/
export interface VolumeFileClusterLibrarySummary extends model.ClusterLibrarySummary {
    /**
    * Library name.
    */
    'name'?: string;
    /**
    * Full path of the library.
    */
    'path'?: string;

   "type": string;
}

export namespace VolumeFileClusterLibrarySummary {



    export function getJsonObj(obj: VolumeFileClusterLibrarySummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterLibrarySummary.getJsonObj(obj) as VolumeFileClusterLibrarySummary, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const type = 'VOLUME_FILE';
    export function getDeserializedJsonObj(obj: VolumeFileClusterLibrarySummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterLibrarySummary.getDeserializedJsonObj(obj) as VolumeFileClusterLibrarySummary, ...{
            


         }};

        
        
        return jsonObj;
    }
}
