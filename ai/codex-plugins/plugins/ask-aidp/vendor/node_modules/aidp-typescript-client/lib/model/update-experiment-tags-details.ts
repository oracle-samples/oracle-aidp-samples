// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the Experiment tags to update.
*/
export interface UpdateExperimentTagsDetails {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;
    /**
    * Experiment tags to set.
    */
    'setTags'?: Array<model.ExperimentTag>;
    /**
    * Experiment tags to delete.
    */
    'deleteTags'?: Array<model.ExperimentTagKey>;

}

export namespace UpdateExperimentTagsDetails {




    export function getJsonObj(obj: UpdateExperimentTagsDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,

                'set_tags': obj.setTags ?
                
                obj.setTags.map((item)=>{return model.ExperimentTag.getJsonObj(item)})
                
                 : undefined,
                'delete_tags': obj.deleteTags ?
                
                obj.deleteTags.map((item)=>{return model.ExperimentTagKey.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<UpdateExperimentTagsDetails>).experimentId;delete (jsonObj as Partial<UpdateExperimentTagsDetails>).setTags;delete (jsonObj as Partial<UpdateExperimentTagsDetails>).deleteTags;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateExperimentTagsDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],

                    'setTags': (obj as any)["set_tags"] ?
                
                (obj as any)["set_tags"].map((item: any)=>{return model.ExperimentTag.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'deleteTags': (obj as any)["delete_tags"] ?
                
                (obj as any)["delete_tags"].map((item: any)=>{return model.ExperimentTagKey.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["experiment_id"];delete (jsonObj as any)["set_tags"];delete (jsonObj as any)["delete_tags"];
        
        return jsonObj;
    }
}
