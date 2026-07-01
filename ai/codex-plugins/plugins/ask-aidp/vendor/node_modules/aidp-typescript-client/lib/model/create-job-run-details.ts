// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a job run.
*/
export interface CreateJobRunDetails {
    /**
    * The OCID of the job.
    */
    'key'?: string;
    /**
    * The OCID of the job.
    */
    'jobKey': string;
    /**
    * The OCID of the job.
    */
    'originalAttemptRunId'?: string;
    'schedule'?: model.Schedule;
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;
    'queue'?: model.Queue;
    /**
    * Array of repaired runs.
    */
    'repairHistory'?: Array<model.RepairHistory>;

}

export namespace CreateJobRunDetails {








    export function getJsonObj(obj: CreateJobRunDetails): object {
        const jsonObj = {...obj, ...{
            



                'schedule': obj.schedule ?
                
                
                model.Schedule.getJsonObj(obj.schedule) : undefined,
                'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getJsonObj(item)})
                
                 : undefined,
                'queue': obj.queue ?
                
                
                model.Queue.getJsonObj(obj.queue) : undefined,
                'repairHistory': obj.repairHistory ?
                
                obj.repairHistory.map((item)=>{return model.RepairHistory.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateJobRunDetails): object {
        const jsonObj = {...obj, ...{
            



                    'schedule': obj.schedule ?
                
                
                model.Schedule.getDeserializedJsonObj(obj.schedule) : undefined,
                    'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'queue': obj.queue ?
                
                
                model.Queue.getDeserializedJsonObj(obj.queue) : undefined,
                    'repairHistory': obj.repairHistory ?
                
                obj.repairHistory.map((item)=>{return model.RepairHistory.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
