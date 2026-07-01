// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of a workspace file installed as a library on a cluster.
*/
export interface WorkspaceFileClusterLibrarySummary extends model.ClusterLibrarySummary {
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

export namespace WorkspaceFileClusterLibrarySummary {



    export function getJsonObj(obj: WorkspaceFileClusterLibrarySummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterLibrarySummary.getJsonObj(obj) as WorkspaceFileClusterLibrarySummary, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const type = 'WORKSPACE_FILE';
    export function getDeserializedJsonObj(obj: WorkspaceFileClusterLibrarySummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterLibrarySummary.getDeserializedJsonObj(obj) as WorkspaceFileClusterLibrarySummary, ...{
            


         }};

        
        
        return jsonObj;
    }
}
