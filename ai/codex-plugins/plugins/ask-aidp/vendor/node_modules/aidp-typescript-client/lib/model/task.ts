// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Properties of a task provided by the user.
*/
export interface Task {
    /**
    * The display name of the task. User can specify a value for this.
    */
    'taskKey': string;
    /**
    * Specifies the dependency graph of the task. All the tasks mentioned in this field need to be completed before executing this task.
    */
    'dependsOn'?: Array<model.DependsOn>;
    /**
    * The trigger rule based on which the current task execution is determined.
    */
    'runIf': Task.RunIf;
    /**
    * The maximum number of times to retry an unsuccessful run. 
* A run is considered to be unsuccessful if it fails with status FAILED or INTERNAL_ERROR. Maximum value is 300.
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxRetries'?: number;
    /**
    * An optional minimal interval in milliseconds between the start of the failed run and the subsequent retry run. 
* If value is not provided, the run would be immediately retried. Maximum value is 10 mins (600000)
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'minRetryIntervalMillis'?: number;
    /**
    * An optional policy to specify whether to retry a task when it times out. The default behavior is to not retry on timeout.
    */
    'isRetryOnTimeout'?: boolean;

   "type": string;
}

export namespace Task {



    export enum RunIf {
    
    AllSuccess = "ALL_SUCCESS",
    AllDone = "ALL_DONE",
    NoneFailed = "NONE_FAILED",
    AtLeastOneSuccess = "AT_LEAST_ONE_SUCCESS",
    AllFailed = "ALL_FAILED",
    AtLeastOneFailed = "AT_LEAST_ONE_FAILED",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}





    export function getJsonObj(obj: Task): object {
        const jsonObj = {...obj, ...{
            

                'dependsOn': obj.dependsOn ?
                
                obj.dependsOn.map((item)=>{return model.DependsOn.getJsonObj(item)})
                
                 : undefined,




        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "IF_ELSE_TASK":
                    return model.IfElseTask.getJsonObj(<model.IfElseTask>(<object>jsonObj), true);
                case "JOB_TASK":
                    return model.JobTask.getJsonObj(<model.JobTask>(<object>jsonObj), true);
                case "JAR_TASK":
                    return model.JarTask.getJsonObj(<model.JarTask>(<object>jsonObj), true);
                case "PYTHON_TASK":
                    return model.PythonTask.getJsonObj(<model.PythonTask>(<object>jsonObj), true);
                case "NOTEBOOK_TASK":
                    return model.NotebookTask.getJsonObj(<model.NotebookTask>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Task): object {
        const jsonObj = {...obj, ...{
            

                    'dependsOn': obj.dependsOn ?
                
                obj.dependsOn.map((item)=>{return model.DependsOn.getDeserializedJsonObj(item)})
                
                 : undefined,




         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "IF_ELSE_TASK":
                    return model.IfElseTask.getDeserializedJsonObj(<model.IfElseTask>(<object>jsonObj), true);
                case "JOB_TASK":
                    return model.JobTask.getDeserializedJsonObj(<model.JobTask>(<object>jsonObj), true);
                case "JAR_TASK":
                    return model.JarTask.getDeserializedJsonObj(<model.JarTask>(<object>jsonObj), true);
                case "PYTHON_TASK":
                    return model.PythonTask.getDeserializedJsonObj(<model.PythonTask>(<object>jsonObj), true);
                case "NOTEBOOK_TASK":
                    return model.NotebookTask.getDeserializedJsonObj(<model.NotebookTask>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
