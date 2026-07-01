// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about the job task.
*/
export interface JobTask extends model.Task {
    /**
    * The OCID of the job.
    */
    'jobKey': string;
    /**
    * An optional list of parameters.
    */
    'parameters'?: Array<model.Parameter>;
    /**
    * An optional value to indicate the max run duration of a job after which job will be timed out. The default is Zero indicating no timeout value. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'timeoutSeconds'?: number;

   "type": string;
}

export namespace JobTask {




    export function getJsonObj(obj: JobTask, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Task.getJsonObj(obj) as JobTask, ...{
            

                'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    export const type = 'JOB_TASK';
    export function getDeserializedJsonObj(obj: JobTask, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Task.getDeserializedJsonObj(obj) as JobTask, ...{
            

                    'parameters': obj.parameters ?
                
                obj.parameters.map((item)=>{return model.Parameter.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
