// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A description of a Job.
* To use any of the API operations, you must be authorized in an IAM policy. If you're not authorized, talk to
* an administrator. If you're an administrator who needs to write policies to give users access, see
* [Getting Started with Policies]({{DOC_SERVER_URL}}/iaas/Content/Identity/policiesgs/get-started-with-policies.htm).
* 
*/
export interface Job {
    /**
    * The OCID of the job.
    */
    'key': string;
    /**
    * The OCID of the IAM user.
    */
    'createdBy': string;
    /**
    * Name of the user who created this record
    */
    'createdByName'?: string;
    /**
    * The username of the latest updater. The OCID of the IAM user.
    */
    'updatedBy'?: string;
    /**
    * Name of the user who updated this record.
    */
    'updatedByName'?: string;
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
    * The path to store the job definition in.
    */
    'path'?: string;
    /**
    * List of job cluster configurations.
    */
    'jobClusters'?: Array<model.JobCluster>;
    /**
    * List of tasks in a job.
    */
    'tasks'?: Array<model.Task>;
    /**
    * The date and time the DataLake was created, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2025-05-25T21:10:29.600Z}
* 
    */
    'timeCreated'?: Date;
    /**
    * The date and time the DataLake was updated, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2025-05-25T21:10:29.600Z}
* 
    */
    'timeUpdated'?: Date;
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;
    /**
    * An optional value to indicate the max run duration of a job after which job will be timed out. The default is Zero indicating no timeout value. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'timeoutSeconds'?: number;

}

export namespace Job {





















    export function getJsonObj(obj: Job): object {
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
    export function getDeserializedJsonObj(obj: Job): object {
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
