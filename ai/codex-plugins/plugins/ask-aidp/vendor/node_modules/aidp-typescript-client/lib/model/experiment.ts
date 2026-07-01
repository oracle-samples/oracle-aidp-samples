// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of an MLflow experiment.
*/
export interface Experiment {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;
    /**
    * Name of the experiment.
    */
    'name': string;
    /**
    * Location where all artifacts for the experiment are stored. If not provided, the remote server will select an appropriate default.
    */
    'artifactLocation'?: string;
    /**
    * Lifecycle stage of the experiment, e.g., 'active' or 'deleted'.
    */
    'lifecycleStage'?: string;
    /**
    * Unix timestamp in milliseconds when the experiment was created. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'creationTime'?: number;
    /**
    * Unix timestamp in milliseconds when the experiment was last updated. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'lastUpdateTime'?: number;
    /**
    * List of tags set on the experiment.
    */
    'tags'?: Array<model.ExperimentTag>;

}

export namespace Experiment {








    export function getJsonObj(obj: Experiment): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,


                'artifact_location': obj.artifactLocation,

                'lifecycle_stage': obj.lifecycleStage,

                'creation_time': obj.creationTime,

                'last_update_time': obj.lastUpdateTime,

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentTag.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<Experiment>).experimentId;delete (jsonObj as Partial<Experiment>).artifactLocation;delete (jsonObj as Partial<Experiment>).lifecycleStage;delete (jsonObj as Partial<Experiment>).creationTime;delete (jsonObj as Partial<Experiment>).lastUpdateTime;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Experiment): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],


                'artifactLocation': (obj as any)["artifact_location"],

                'lifecycleStage': (obj as any)["lifecycle_stage"],

                'creationTime': (obj as any)["creation_time"],

                'lastUpdateTime': (obj as any)["last_update_time"],

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentTag.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["experiment_id"];delete (jsonObj as any)["artifact_location"];delete (jsonObj as any)["lifecycle_stage"];delete (jsonObj as any)["creation_time"];delete (jsonObj as any)["last_update_time"];
        
        return jsonObj;
    }
}
