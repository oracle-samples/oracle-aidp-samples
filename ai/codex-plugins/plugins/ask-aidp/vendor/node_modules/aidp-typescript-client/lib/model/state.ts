// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The current state of the run.
*/
export interface State {
    /**
    * Current state of execution.
    */
    'status'?: State.Status;
    /**
    * A descriptive message of the current state.
    */
    'stateMessage'?: string;
    /**
    * If there was an error executing the run, this field contains any available stack traces.
    */
    'errorTrace'?: string;
    /**
    * Set to true, if the job execution is canceled by the user or by the scheduler due to timeout.
    */
    'isUserCanceledOrTimedOut'?: boolean;

}

export namespace State {

    export enum Status {
    
    Pending = "PENDING",
    Queued = "QUEUED",
    Running = "RUNNING",
    Skipped = "SKIPPED",
    InternalError = "INTERNAL_ERROR",
    Blocked = "BLOCKED",
    Success = "SUCCESS",
    Failed = "FAILED",
    Canceling = "CANCELING",
    Canceled = "CANCELED",
    UpstreamCanceled = "UPSTREAM_CANCELED",
    UpstreamFailed = "UPSTREAM_FAILED",
    Excluded = "EXCLUDED",
    TimedOut = "TIMED_OUT",
    PausedMaintenance = "PAUSED_MAINTENANCE",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}





    export function getJsonObj(obj: State): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: State): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
