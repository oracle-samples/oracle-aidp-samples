// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of run to create.
*/
export interface CreateExperimentRunDetails {
    /**
    * Name of the run.
    */
    'runName'?: string;
    /**
    * ID of the associated experiment.
    */
    'experimentId'?: string;
    /**
    * Unix timestamp in milliseconds when the run started. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime'?: number;
    /**
    * Tags to log.
    */
    'tags'?: Array<model.ExperimentRunTag>;

}

export namespace CreateExperimentRunDetails {





    export function getJsonObj(obj: CreateExperimentRunDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_name': obj.runName,

                'experiment_id': obj.experimentId,

                'start_time': obj.startTime,

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentRunTag.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<CreateExperimentRunDetails>).runName;delete (jsonObj as Partial<CreateExperimentRunDetails>).experimentId;delete (jsonObj as Partial<CreateExperimentRunDetails>).startTime;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateExperimentRunDetails): object {
        const jsonObj = {...obj, ...{
            
                'runName': (obj as any)["run_name"],

                'experimentId': (obj as any)["experiment_id"],

                'startTime': (obj as any)["start_time"],

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentRunTag.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["run_name"];delete (jsonObj as any)["experiment_id"];delete (jsonObj as any)["start_time"];
        
        return jsonObj;
    }
}
