// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The continuous property ensures that there is always one execution for this job.
*/
export interface Continuous {
    /**
    * Indicates whether the continuous execution of this job is paused or not.
    */
    'pauseStatus'?: Continuous.PauseStatus;

}

export namespace Continuous {

    export enum PauseStatus {
    
    Paused = "PAUSED",
    Unpaused = "UNPAUSED",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: Continuous): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Continuous): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
