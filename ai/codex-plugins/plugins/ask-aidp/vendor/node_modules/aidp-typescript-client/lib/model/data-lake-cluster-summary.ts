// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about the list of AI Data Platform Workbench clusters contained within a workspace.
*/
export interface DataLakeClusterSummary {
    /**
    * Key of the AI Data Platform Workbench workspace.
    */
    'workspaceKey': string;
    /**
    * Name of the AI Data Platform Workbench workspace.
    */
    'workspaceDisplayName': string;
    /**
    * List of clusters.
    */
    'clusters'?: Array<model.ClusterSummary>;

}

export namespace DataLakeClusterSummary {




    export function getJsonObj(obj: DataLakeClusterSummary): object {
        const jsonObj = {...obj, ...{
            


                'clusters': obj.clusters ?
                
                obj.clusters.map((item)=>{return model.ClusterSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DataLakeClusterSummary): object {
        const jsonObj = {...obj, ...{
            


                    'clusters': obj.clusters ?
                
                obj.clusters.map((item)=>{return model.ClusterSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
