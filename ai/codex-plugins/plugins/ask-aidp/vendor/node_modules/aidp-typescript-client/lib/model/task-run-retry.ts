// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Representation of a task run retry. Maximum of 10 retries are allowed.
*/
export interface TaskRunRetry {
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime'?: number;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime'?: number;
    /**
    * The time at which the cluster validation started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'clusterValidationStartTime'?: number;
    'state'?: model.State;
    /**
    * The time (in milliseconds) taken to setup the cluster. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'setupDuration'?: number;
    /**
    * The time (in milliseconds) taken to complete the job execution. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'executionDuration'?: number;
    /**
    * The time (in milliseconds) taken to terminate the cluster and to clean up any associated artifacts. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'cleanupDuration'?: number;
    /**
    * A unique identifier for the output.
    */
    'outputKey'?: string;
    /**
    * The external ID of the task execution.
    */
    'externalId'?: string;
    /**
    * Sequence number of the retry attempt. 0 for the original. 1, 2, 3, ... for subsequent retry attempts. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'retryNumber'?: number;

}

export namespace TaskRunRetry {











    export function getJsonObj(obj: TaskRunRetry): object {
        const jsonObj = {...obj, ...{
            



                'state': obj.state ?
                
                
                model.State.getJsonObj(obj.state) : undefined,






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TaskRunRetry): object {
        const jsonObj = {...obj, ...{
            



                    'state': obj.state ?
                
                
                model.State.getDeserializedJsonObj(obj.state) : undefined,






         }};

        
        
        return jsonObj;
    }
}
