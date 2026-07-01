// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Output of a task run.
*/
export interface TaskRunOutput {
    /**
    * A unique identifier for the output.
    */
    'key'?: string;
    /**
    * The type of the task.
    */
    'taskType'?: TaskRunOutput.TaskType;
    /**
    * True if logs are truncated.
    */
    'isTruncated'?: boolean;
    /**
    * If there was an error executing the run, this field contains any available stack traces.
    */
    'errorTrace'?: string;
    /**
    * Array of output objects.
    */
    'data': Array<model.RunOutputData>;
    /**
    * List of output parameters with name and values.
    */
    'outputParameters'?: Array<model.OutputParameter>;
    /**
    * Current version of job run object in repository. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'version'?: number;

}

export namespace TaskRunOutput {


    export enum TaskType {
    
    NotebookTask = "NOTEBOOK_TASK",
    PythonTask = "PYTHON_TASK",
    SparkSubmitTask = "SPARK_SUBMIT_TASK",
    IfElseTask = "IF_ELSE_TASK",
    JobTask = "JOB_TASK",
    JarTask = "JAR_TASK",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}







    export function getJsonObj(obj: TaskRunOutput): object {
        const jsonObj = {...obj, ...{
            




                'data': obj.data ?
                
                obj.data.map((item)=>{return model.RunOutputData.getJsonObj(item)})
                
                 : undefined,
                'outputParameters': obj.outputParameters ?
                
                obj.outputParameters.map((item)=>{return model.OutputParameter.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TaskRunOutput): object {
        const jsonObj = {...obj, ...{
            




                    'data': obj.data ?
                
                obj.data.map((item)=>{return model.RunOutputData.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'outputParameters': obj.outputParameters ?
                
                obj.outputParameters.map((item)=>{return model.OutputParameter.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
