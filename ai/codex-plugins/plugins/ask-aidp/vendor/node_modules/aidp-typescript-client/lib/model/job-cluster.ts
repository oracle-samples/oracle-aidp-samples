// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The cluster configuration that can be shared by tasks in the job.
*/
export interface JobCluster {
    /**
    * A unique identifier for the job cluster.
    */
    'clusterKey'?: string;
    /**
    * A unique name for the job cluster.
    */
    'clusterName'?: string;
    'newCluster'?: model.NewClusterConfiguration;

}

export namespace JobCluster {




    export function getJsonObj(obj: JobCluster): object {
        const jsonObj = {...obj, ...{
            


                'newCluster': obj.newCluster ?
                
                
                model.NewClusterConfiguration.getJsonObj(obj.newCluster) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: JobCluster): object {
        const jsonObj = {...obj, ...{
            


                    'newCluster': obj.newCluster ?
                
                
                model.NewClusterConfiguration.getDeserializedJsonObj(obj.newCluster) : undefined,
         }};

        
        
        return jsonObj;
    }
}
