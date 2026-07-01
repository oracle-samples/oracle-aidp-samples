// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a model version.
*/
export interface CreateModelVersionDetails {
    /**
    * Register models under this name.
    */
    'name': string;
    /**
    * Description of this model version.
    */
    'description'?: string;
    /**
    * URI indicating the location of the source model artifacts.
    */
    'source': string;
    /**
    * Run ID that generated the model version.
    */
    'runId'?: string;
    /**
    * Tags of model version.
    */
    'tags'?: Array<model.ModelVersionTag>;
    /**
    *  Direct link to the run that generated this version.
    */
    'runLink'?: string;
    /**
    * Model ID for model version that is used to link the registered model to the source logged model.
    */
    'modelId'?: string;

}

export namespace CreateModelVersionDetails {








    export function getJsonObj(obj: CreateModelVersionDetails): object {
        const jsonObj = {...obj, ...{
            



                'run_id': obj.runId,

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ModelVersionTag.getJsonObj(item)})
                
                 : undefined,
                'run_link': obj.runLink,

                'model_id': obj.modelId,

        }};

        delete (jsonObj as Partial<CreateModelVersionDetails>).runId;delete (jsonObj as Partial<CreateModelVersionDetails>).runLink;delete (jsonObj as Partial<CreateModelVersionDetails>).modelId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateModelVersionDetails): object {
        const jsonObj = {...obj, ...{
            



                'runId': (obj as any)["run_id"],

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ModelVersionTag.getDeserializedJsonObj(item)})
                
                 : undefined,
                'runLink': (obj as any)["run_link"],

                'modelId': (obj as any)["model_id"],

         }};

        delete (jsonObj as any)["run_id"];delete (jsonObj as any)["run_link"];delete (jsonObj as any)["model_id"];
        
        return jsonObj;
    }
}
