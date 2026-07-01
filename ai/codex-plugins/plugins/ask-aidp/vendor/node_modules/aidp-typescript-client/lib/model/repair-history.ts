// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A description of a repaired job run.
*/
export interface RepairHistory {
    /**
    * Indicates whether the job run is Original or Repaired.
    */
    'type'?: RepairHistory.Type;
    /**
    * The unique ID of the Repair run. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'key'?: number;
    'state'?: model.State;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime'?: number;
    /**
    * The time at which the job execution started in epoch milliseconds. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime'?: number;
    /**
    * Task to TaskRun map for given job run.
    */
    'taskToTaskRunMap'?: { [key: string]: string; };
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;
    /**
    * A list of repaired tasks.
    */
    'repairedTasks'?: Array<string>;
    /**
    * The collection of lifecycle states.
    */
    'lifecycleStates'?: Array<model.LifecycleState>;
    /**
    * The time (in milliseconds) taken to complete the job execution. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'executionDuration'?: number;

}

export namespace RepairHistory {

    export enum Type {
    
    Original = "ORIGINAL",
    Repair = "REPAIR",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}











    export function getJsonObj(obj: RepairHistory): object {
        const jsonObj = {...obj, ...{
            


                'state': obj.state ?
                
                
                model.State.getJsonObj(obj.state) : undefined,



                'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getJsonObj(item)})
                
                 : undefined,

                'lifecycleStates': obj.lifecycleStates ?
                
                obj.lifecycleStates.map((item)=>{return model.LifecycleState.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RepairHistory): object {
        const jsonObj = {...obj, ...{
            


                    'state': obj.state ?
                
                
                model.State.getDeserializedJsonObj(obj.state) : undefined,



                    'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getDeserializedJsonObj(item)})
                
                 : undefined,

                    'lifecycleStates': obj.lifecycleStates ?
                
                obj.lifecycleStates.map((item)=>{return model.LifecycleState.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
