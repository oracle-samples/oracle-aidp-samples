// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a job run.
*/
export interface JobRunSummary {
    /**
    * The OCID of the job run.
    */
    'key': string;
    /**
    * A user-friendly name. Does not have to be unique, and is changeable.
    */
    'name'?: string;
    'state'?: model.State;
    /**
    * The OCID of the job.
    */
    'jobKey'?: string;
    /**
    * A user-friendly name. Does not have to be unique, and is changeable.
    */
    'jobName'?: string;
    /**
    * The time (in milliseconds) taken to complete the job execution. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'executionDuration'?: number;
    /**
    * Identify job run launched by schedule or manually.
    */
    'launched'?: JobRunSummary.Launched;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime'?: number;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime'?: number;
    /**
    * The time at which the job execution started.
    */
    'timeCreated'?: Date;
    /**
    * The time at which the job execution was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The user who triggered the job execution.
    */
    'createdBy'?: string;
    /**
    * Name of the user who created this record
    */
    'createdByName'?: string;

}

export namespace JobRunSummary {







    export enum Launched {
    
    Scheduled = "SCHEDULED",
    Manual = "MANUAL",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}








    export function getJsonObj(obj: JobRunSummary): object {
        const jsonObj = {...obj, ...{
            


                'state': obj.state ?
                
                
                model.State.getJsonObj(obj.state) : undefined,










        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: JobRunSummary): object {
        const jsonObj = {...obj, ...{
            


                    'state': obj.state ?
                
                
                model.State.getDeserializedJsonObj(obj.state) : undefined,










         }};

        
        
        return jsonObj;
    }
}
