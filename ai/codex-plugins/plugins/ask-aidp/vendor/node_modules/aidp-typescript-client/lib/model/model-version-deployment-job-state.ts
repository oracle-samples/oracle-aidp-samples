// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* ModelVersion tag.
*/
export interface ModelVersionDeploymentJobState {
    /**
    * Job ID.
    */
    'jobId'?: string;
    /**
    * Run ID.
    */
    'runId'?: string;
    /**
    * Job state.
    */
    'jobState'?: model.DeploymentJobState;
    /**
    * Run state.
    */
    'runState'?: model.DeploymentJobRunState;
    /**
    * Current task name.
    */
    'currentTaskName'?: string;

}

export namespace ModelVersionDeploymentJobState {






    export function getJsonObj(obj: ModelVersionDeploymentJobState): object {
        const jsonObj = {...obj, ...{
            
                'job_id': obj.jobId,

                'run_id': obj.runId,

                'job_state': obj.jobState,

                'run_state': obj.runState,

                'current_task_name': obj.currentTaskName,

        }};

        delete (jsonObj as Partial<ModelVersionDeploymentJobState>).jobId;delete (jsonObj as Partial<ModelVersionDeploymentJobState>).runId;delete (jsonObj as Partial<ModelVersionDeploymentJobState>).jobState;delete (jsonObj as Partial<ModelVersionDeploymentJobState>).runState;delete (jsonObj as Partial<ModelVersionDeploymentJobState>).currentTaskName;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelVersionDeploymentJobState): object {
        const jsonObj = {...obj, ...{
            
                'jobId': (obj as any)["job_id"],

                'runId': (obj as any)["run_id"],

                'jobState': (obj as any)["job_state"],

                'runState': (obj as any)["run_state"],

                'currentTaskName': (obj as any)["current_task_name"],

         }};

        delete (jsonObj as any)["job_id"];delete (jsonObj as any)["run_id"];delete (jsonObj as any)["job_state"];delete (jsonObj as any)["run_state"];delete (jsonObj as any)["current_task_name"];
        
        return jsonObj;
    }
}
