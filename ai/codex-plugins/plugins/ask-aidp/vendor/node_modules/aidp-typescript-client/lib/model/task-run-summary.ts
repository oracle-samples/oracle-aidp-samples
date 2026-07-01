// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a TaskRun.
*/
export interface TaskRunSummary {
    /**
    * The OCID of the TaskRun.
    */
    'key'?: string;
    /**
    * A user-friendly name. Does not have to be unique, and is changeable.
    */
    'name'?: string;
    /**
    * The display name of the task. User can specify a value for this.
    */
    'taskKey'?: string;
    /**
    * The OCID of the job.
    */
    'jobRunKey'?: string;
    /**
    * The OCID of the job.
    */
    'parentJobRunKey'?: string;
    /**
    * The OCID of the job.
    */
    'rootJobRunKey'?: string;
    /**
    * The time at which the cluster validation started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'clusterValidationStartTime'?: number;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime'?: number;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime'?: number;
    'state'?: model.State;
    /**
    * The external ID of the task execution.
    */
    'externalId'?: string;
    /**
    * Sequence number of the current retry attempt. 0 for the original. 1, 2, 3, ... for subsequent retry attempts. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'retryAttempt'?: number;

}

export namespace TaskRunSummary {













    export function getJsonObj(obj: TaskRunSummary): object {
        const jsonObj = {...obj, ...{
            









                'state': obj.state ?
                
                
                model.State.getJsonObj(obj.state) : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TaskRunSummary): object {
        const jsonObj = {...obj, ...{
            









                    'state': obj.state ?
                
                
                model.State.getDeserializedJsonObj(obj.state) : undefined,


         }};

        
        
        return jsonObj;
    }
}
