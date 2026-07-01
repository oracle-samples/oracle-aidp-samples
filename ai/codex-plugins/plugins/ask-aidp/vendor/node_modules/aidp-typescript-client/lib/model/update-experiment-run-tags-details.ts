// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the ExperimentRun tags to update.
*/
export interface UpdateExperimentRunTagsDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * ExperimentRun tags to set
    */
    'setTags'?: Array<model.ExperimentRunTag>;
    /**
    * ExperimentRun tags to delete
    */
    'deleteTags'?: Array<model.ExperimentRunTagKey>;

}

export namespace UpdateExperimentRunTagsDetails {




    export function getJsonObj(obj: UpdateExperimentRunTagsDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,

                'set_tags': obj.setTags ?
                
                obj.setTags.map((item)=>{return model.ExperimentRunTag.getJsonObj(item)})
                
                 : undefined,
                'delete_tags': obj.deleteTags ?
                
                obj.deleteTags.map((item)=>{return model.ExperimentRunTagKey.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<UpdateExperimentRunTagsDetails>).runId;delete (jsonObj as Partial<UpdateExperimentRunTagsDetails>).setTags;delete (jsonObj as Partial<UpdateExperimentRunTagsDetails>).deleteTags;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateExperimentRunTagsDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],

                    'setTags': (obj as any)["set_tags"] ?
                
                (obj as any)["set_tags"].map((item: any)=>{return model.ExperimentRunTag.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'deleteTags': (obj as any)["delete_tags"] ?
                
                (obj as any)["delete_tags"].map((item: any)=>{return model.ExperimentRunTagKey.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["run_id"];delete (jsonObj as any)["set_tags"];delete (jsonObj as any)["delete_tags"];
        
        return jsonObj;
    }
}
