// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a job.
*/
export interface JobSummary {
    /**
    * The OCID of the job.
    */
    'key': string;
    /**
    * The OCID of the IAM user.
    */
    'createdBy'?: string;
    /**
    * Name of the user who created this record
    */
    'createdByName'?: string;
    /**
    * The OCID of the IAM user.
    */
    'updatedBy'?: string;
    /**
    * The username of the latest updater.
    */
    'updatedByName'?: string;
    /**
    * A user-friendly name. Does not have to be unique, and is changeable.
    */
    'name'?: string;
    /**
    * The path to store the job definition in.
    */
    'path'?: string;
    'schedule'?: model.Schedule;
    /**
    * The id with which the job run as.
    */
    'runAs'?: string;
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
    * List of job cluster keys.
    */
    'clusters'?: Array<string>;
    /**
    * List of job cluster configurations.
    */
    'jobClusters'?: Array<model.JobCluster>;

}

export namespace JobSummary {














    export function getJsonObj(obj: JobSummary): object {
        const jsonObj = {...obj, ...{
            







                'schedule': obj.schedule ?
                
                
                model.Schedule.getJsonObj(obj.schedule) : undefined,




                'jobClusters': obj.jobClusters ?
                
                obj.jobClusters.map((item)=>{return model.JobCluster.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: JobSummary): object {
        const jsonObj = {...obj, ...{
            







                    'schedule': obj.schedule ?
                
                
                model.Schedule.getDeserializedJsonObj(obj.schedule) : undefined,




                    'jobClusters': obj.jobClusters ?
                
                obj.jobClusters.map((item)=>{return model.JobCluster.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
