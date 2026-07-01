// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Active resources of a cluster.
*/
export interface ActiveClusterResources {
    /**
    * Count of active executors. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'activeExecutorCount'?: number;
    /**
    * Count of active cores. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'activeCores'?: number;
    /**
    * Count of active GPU cores. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'activeGpuCores'?: number;
    /**
    * Active memory in GB. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'activeMemoryInGBs'?: number;
    /**
    * Active GPU memory in GB. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'activeGpuMemoryInGBs'?: number;

}

export namespace ActiveClusterResources {






    export function getJsonObj(obj: ActiveClusterResources): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ActiveClusterResources): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
