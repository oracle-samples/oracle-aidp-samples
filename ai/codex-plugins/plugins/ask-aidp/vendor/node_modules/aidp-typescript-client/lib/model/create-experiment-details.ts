// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create an experiment.
*/
export interface CreateExperimentDetails {
    /**
    * Name of the experiment.
    */
    'name': string;
    /**
    * Location where all artifacts for the experiment are stored. If not provided, the remote server will select an appropriate default.
    */
    'artifactLocation'?: string;
    /**
    * List of tags set on the experiment.
    */
    'tags'?: Array<model.ExperimentTag>;

}

export namespace CreateExperimentDetails {




    export function getJsonObj(obj: CreateExperimentDetails): object {
        const jsonObj = {...obj, ...{
            

                'artifact_location': obj.artifactLocation,

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentTag.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<CreateExperimentDetails>).artifactLocation;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateExperimentDetails): object {
        const jsonObj = {...obj, ...{
            

                'artifactLocation': (obj as any)["artifact_location"],

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentTag.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["artifact_location"];
        
        return jsonObj;
    }
}
