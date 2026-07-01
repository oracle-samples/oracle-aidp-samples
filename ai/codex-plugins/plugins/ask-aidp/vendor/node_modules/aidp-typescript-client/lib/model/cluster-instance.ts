// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The cluster used for this run.
* The value of this field will be set when a new cluster is specified for execution and once the request to create a new cluster is successfully submitted.
* 
*/
export interface ClusterInstance {
    /**
    * The cluster key for the cluster configuration on which the job is executed.
    */
    'clusterKey'?: string;
    /**
    * The spark context used in the job run.
    */
    'sparkContextKey'?: string;

}

export namespace ClusterInstance {



    export function getJsonObj(obj: ClusterInstance): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterInstance): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
