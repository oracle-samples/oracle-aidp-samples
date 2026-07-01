// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the logged model info.
*/
export interface LoggedModelInfo {
    /**
    * ID of logged model.
    */
    'modelId'?: string;
    /**
    * Unique identifier for the experiment.
    */
    'experimentId'?: string;
    /**
    * Name of logged model.
    */
    'name'?: string;
    /**
    * Unix timestamp in milliseconds when the logged model was created. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'creationTimestampMs'?: number;
    /**
    * Unix timestamp in milliseconds when the logged model was last updated. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'lastUpdatedTimestampMs'?: number;
    /**
    * Artifact URI.
    */
    'artifactUri'?: string;
    /**
    * Status of logged model.
    */
    'status'?: string;
    /**
    * Model type of logged model.
    */
    'modelType'?: string;
    /**
    * Source run ID of logged model.
    */
    'sourceRunId'?: string;
    /**
    * Tags of logged model.
    */
    'tags'?: Array<model.LoggedModelTag>;

}

export namespace LoggedModelInfo {











    export function getJsonObj(obj: LoggedModelInfo): object {
        const jsonObj = {...obj, ...{
            
                'model_id': obj.modelId,

                'experiment_id': obj.experimentId,


                'creation_timestamp_ms': obj.creationTimestampMs,

                'last_updated_timestamp_ms': obj.lastUpdatedTimestampMs,

                'artifact_uri': obj.artifactUri,


                'model_type': obj.modelType,

                'source_run_id': obj.sourceRunId,

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.LoggedModelTag.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<LoggedModelInfo>).modelId;delete (jsonObj as Partial<LoggedModelInfo>).experimentId;delete (jsonObj as Partial<LoggedModelInfo>).creationTimestampMs;delete (jsonObj as Partial<LoggedModelInfo>).lastUpdatedTimestampMs;delete (jsonObj as Partial<LoggedModelInfo>).artifactUri;delete (jsonObj as Partial<LoggedModelInfo>).modelType;delete (jsonObj as Partial<LoggedModelInfo>).sourceRunId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LoggedModelInfo): object {
        const jsonObj = {...obj, ...{
            
                'modelId': (obj as any)["model_id"],

                'experimentId': (obj as any)["experiment_id"],


                'creationTimestampMs': (obj as any)["creation_timestamp_ms"],

                'lastUpdatedTimestampMs': (obj as any)["last_updated_timestamp_ms"],

                'artifactUri': (obj as any)["artifact_uri"],


                'modelType': (obj as any)["model_type"],

                'sourceRunId': (obj as any)["source_run_id"],

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.LoggedModelTag.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["model_id"];delete (jsonObj as any)["experiment_id"];delete (jsonObj as any)["creation_timestamp_ms"];delete (jsonObj as any)["last_updated_timestamp_ms"];delete (jsonObj as any)["artifact_uri"];delete (jsonObj as any)["model_type"];delete (jsonObj as any)["source_run_id"];
        
        return jsonObj;
    }
}
