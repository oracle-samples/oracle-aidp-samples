// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a job.
*/
export interface CreateJobDetails {
    /**
    * The id with which the job run as.
    */
    'runAs'?: string;
    /**
    * A user-friendly name. Does not have to be unique, and is changeable.
    */
    'name': string;
    /**
    * A description for the job.
    */
    'description'?: string;
    'schedule'?: model.Schedule;
    'continuous'?: model.Continuous;
    /**
    * Indicates the number of executions for the same job that can be run concurrently. The maximum value cannot exceed 1000. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxConcurrentRuns'?: number;
    'gitConfig'?: model.GitConfig;
    'queue'?: model.Queue;
    /**
    * List of job cluster configurations.
    */
    'jobClusters'?: Array<model.JobCluster>;
    /**
    * The path to store the job definition in.
    */
    'path'?: string;
    /**
    * List of tasks in a job.
    */
    'tasks'?: Array<model.Task>;
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;
    /**
    * An optional value to indicate the max run duration of a job after which job will be timed out. The default is Zero indicating no timeout value. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'timeoutSeconds'?: number;

}

export namespace CreateJobDetails {














    export function getJsonObj(obj: CreateJobDetails): object {
        const jsonObj = {...obj, ...{
            



                'schedule': obj.schedule ?
                
                
                model.Schedule.getJsonObj(obj.schedule) : undefined,
                'continuous': obj.continuous ?
                
                
                model.Continuous.getJsonObj(obj.continuous) : undefined,

                'gitConfig': obj.gitConfig ?
                
                
                model.GitConfig.getJsonObj(obj.gitConfig) : undefined,
                'queue': obj.queue ?
                
                
                model.Queue.getJsonObj(obj.queue) : undefined,
                'jobClusters': obj.jobClusters ?
                
                obj.jobClusters.map((item)=>{return model.JobCluster.getJsonObj(item)})
                
                 : undefined,

                'tasks': obj.tasks ?
                
                obj.tasks.map((item)=>{return model.Task.getJsonObj(item)})
                
                 : undefined,
                'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateJobDetails): object {
        const jsonObj = {...obj, ...{
            



                    'schedule': obj.schedule ?
                
                
                model.Schedule.getDeserializedJsonObj(obj.schedule) : undefined,
                    'continuous': obj.continuous ?
                
                
                model.Continuous.getDeserializedJsonObj(obj.continuous) : undefined,

                    'gitConfig': obj.gitConfig ?
                
                
                model.GitConfig.getDeserializedJsonObj(obj.gitConfig) : undefined,
                    'queue': obj.queue ?
                
                
                model.Queue.getDeserializedJsonObj(obj.queue) : undefined,
                    'jobClusters': obj.jobClusters ?
                
                obj.jobClusters.map((item)=>{return model.JobCluster.getDeserializedJsonObj(item)})
                
                 : undefined,

                    'tasks': obj.tasks ?
                
                obj.tasks.map((item)=>{return model.Task.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
