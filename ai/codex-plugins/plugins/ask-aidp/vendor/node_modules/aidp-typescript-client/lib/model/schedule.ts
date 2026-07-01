// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The schedule configuration for the job.
*/
export interface Schedule {
    /**
    * A cron expression using Quartz syntax that describes the schedule for a job.
    */
    'quartzCronExpression': string;
    /**
    * A Java timezone ID. The schedule of the job is resolved with respect to this timezone. Example - US/Pacific.
    */
    'timezoneId': string;
    /**
    * Indicates whether the schedule is paused or not.
    */
    'pauseStatus'?: Schedule.PauseStatus;

}

export namespace Schedule {



    export enum PauseStatus {
    
    Paused = "PAUSED",
    Unpaused = "UNPAUSED",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: Schedule): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Schedule): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
