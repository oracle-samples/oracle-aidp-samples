// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A description of a task run.
*/
export interface TaskRun {
    /**
    * The OCID of the task run.
    */
    'key': string;
    /**
    * A user-friendly name. Does not have to be unique, and is changeable.
    */
    'name': string;
    /**
    * The display name of the task. User can specify a value for this.
    */
    'taskKey'?: string;
    /**
    * The OCID of the job.
    */
    'jobKey'?: string;
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
    'clusterSpec'?: model.ClusterSpec;
    'task'?: model.IfElseTask| model.JobTask| model.JarTask| model.PythonTask| model.NotebookTask;
    /**
    * Current version of job run object in repository. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'version'?: number;
    /**
    * Sequence number of the current retry attempt. 0 for the original. 1, 2, 3, ... for subsequent retry attempts. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'retryAttempt'?: number;
    /**
    * List of task retries.
    */
    'retries'?: Array<model.TaskRunRetry>;
    /**
    * A unique identifier for the output.
    */
    'outputKey'?: string;
    /**
    * The external ID of the task execution.
    */
    'externalId'?: string;
    /**
    * Map of system parameters with their values for this job run.
    */
    'systemParameters'?: { [key: string]: string; };
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;
    /**
    * The collection of lifecycle states.
    */
    'lifecycleStates'?: Array<model.LifecycleState>;

}

export namespace TaskRun {

























    export function getJsonObj(obj: TaskRun): object {
        const jsonObj = {...obj, ...{
            










                'state': obj.state ?
                
                
                model.State.getJsonObj(obj.state) : undefined,



                'clusterSpec': obj.clusterSpec ?
                
                
                model.ClusterSpec.getJsonObj(obj.clusterSpec) : undefined,
                'task': obj.task ?
                
                
                model.Task.getJsonObj(obj.task) : undefined,


                'retries': obj.retries ?
                
                obj.retries.map((item)=>{return model.TaskRunRetry.getJsonObj(item)})
                
                 : undefined,



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
    export function getDeserializedJsonObj(obj: TaskRun): object {
        const jsonObj = {...obj, ...{
            










                    'state': obj.state ?
                
                
                model.State.getDeserializedJsonObj(obj.state) : undefined,



                    'clusterSpec': obj.clusterSpec ?
                
                
                model.ClusterSpec.getDeserializedJsonObj(obj.clusterSpec) : undefined,
                    'task': obj.task ?
                
                
                model.Task.getDeserializedJsonObj(obj.task) : undefined,


                    'retries': obj.retries ?
                
                obj.retries.map((item)=>{return model.TaskRunRetry.getDeserializedJsonObj(item)})
                
                 : undefined,



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
