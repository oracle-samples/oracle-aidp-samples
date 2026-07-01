// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run metadata.
*/
export interface ExperimentRunInfo {
    /**
    * Unique identifier for the run.
    */
    'runId'?: string;
    /**
    * Name of the run.
    */
    'runName'?: string;
    /**
    * ID of the associated experiment.
    */
    'experimentId'?: string;
    /**
    * Status of the run.
    */
    'status'?: model.ExperimentRunStatus;
    /**
    * Unix timestamp in milliseconds when the run started. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime'?: number;
    /**
    * Unix timestamp in milliseconds when the run ended. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime'?: number;
    /**
    * Artifact URI of the run.
    */
    'artifactUri'?: string;
    /**
    * Lifecycle stage of the experiment, e.g., 'active' or 'deleted'.
    */
    'lifecycleStage'?: string;
    /**
    * UUID of the run.
    */
    'runUuid'?: string;
    /**
    * User ID that created the run.
    */
    'userId'?: string;

}

export namespace ExperimentRunInfo {











    export function getJsonObj(obj: ExperimentRunInfo): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,

                'run_name': obj.runName,

                'experiment_id': obj.experimentId,


                'start_time': obj.startTime,

                'end_time': obj.endTime,

                'artifact_uri': obj.artifactUri,

                'lifecycle_stage': obj.lifecycleStage,

                'run_uuid': obj.runUuid,

                'user_id': obj.userId,

        }};

        delete (jsonObj as Partial<ExperimentRunInfo>).runId;delete (jsonObj as Partial<ExperimentRunInfo>).runName;delete (jsonObj as Partial<ExperimentRunInfo>).experimentId;delete (jsonObj as Partial<ExperimentRunInfo>).startTime;delete (jsonObj as Partial<ExperimentRunInfo>).endTime;delete (jsonObj as Partial<ExperimentRunInfo>).artifactUri;delete (jsonObj as Partial<ExperimentRunInfo>).lifecycleStage;delete (jsonObj as Partial<ExperimentRunInfo>).runUuid;delete (jsonObj as Partial<ExperimentRunInfo>).userId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunInfo): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],

                'runName': (obj as any)["run_name"],

                'experimentId': (obj as any)["experiment_id"],


                'startTime': (obj as any)["start_time"],

                'endTime': (obj as any)["end_time"],

                'artifactUri': (obj as any)["artifact_uri"],

                'lifecycleStage': (obj as any)["lifecycle_stage"],

                'runUuid': (obj as any)["run_uuid"],

                'userId': (obj as any)["user_id"],

         }};

        delete (jsonObj as any)["run_id"];delete (jsonObj as any)["run_name"];delete (jsonObj as any)["experiment_id"];delete (jsonObj as any)["start_time"];delete (jsonObj as any)["end_time"];delete (jsonObj as any)["artifact_uri"];delete (jsonObj as any)["lifecycle_stage"];delete (jsonObj as any)["run_uuid"];delete (jsonObj as any)["user_id"];
        
        return jsonObj;
    }
}
