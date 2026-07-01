// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Cluster specification snapshot of a job.
*/
export interface ClusterSpec {
    /**
    * The key of the cluster.
    */
    'existingClusterKey'?: string;
    'newCluster'?: model.NewClusterConfiguration;
    'libraries'?: model.Libraries;

}

export namespace ClusterSpec {




    export function getJsonObj(obj: ClusterSpec): object {
        const jsonObj = {...obj, ...{
            

                'newCluster': obj.newCluster ?
                
                
                model.NewClusterConfiguration.getJsonObj(obj.newCluster) : undefined,
                'libraries': obj.libraries ?
                
                
                model.Libraries.getJsonObj(obj.libraries) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterSpec): object {
        const jsonObj = {...obj, ...{
            

                    'newCluster': obj.newCluster ?
                
                
                model.NewClusterConfiguration.getDeserializedJsonObj(obj.newCluster) : undefined,
                    'libraries': obj.libraries ?
                
                
                model.Libraries.getDeserializedJsonObj(obj.libraries) : undefined,
         }};

        
        
        return jsonObj;
    }
}
