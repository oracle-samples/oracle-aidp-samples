// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Task run execution result.
*/
export interface TaskExecutionResult {
    'state': model.State;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTimeMillis'?: number;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTimeMillis'?: number;
    /**
    * The external ID of the task execution.
    */
    'externalId'?: string;
    /**
    * The metrics of the task execution.
    */
    'metrics'?: { [key: string]: string; };
    /**
    * The properties of the task execution.
    */
    'properties'?: { [key: string]: any; };

}

export namespace TaskExecutionResult {







    export function getJsonObj(obj: TaskExecutionResult): object {
        const jsonObj = {...obj, ...{
            
                'state': obj.state ?
                
                
                model.State.getJsonObj(obj.state) : undefined,





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TaskExecutionResult): object {
        const jsonObj = {...obj, ...{
            
                    'state': obj.state ?
                
                
                model.State.getDeserializedJsonObj(obj.state) : undefined,





         }};

        
        
        return jsonObj;
    }
}
