// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the model version.
*/
export interface ModelVersion {
    /**
    * Unique name for the model.
    */
    'name'?: string;
    /**
    * Model\u2019s version number.
    */
    'version'?: string;
    /**
    * Timestamp in milliseconds when the model version was created. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'creationTimestamp'?: number;
    /**
    * Timestamp in milliseconds when metadata for the model version was last updated. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'lastUpdatedTimestamp'?: number;
    /**
    * User that created this model version.
    */
    'userId'?: string;
    /**
    * Current stage for this model version.
    */
    'currentStage'?: string;
    /**
    * Description of this model version.
    */
    'description'?: string;
    /**
    * URI indicating the location of the source model artifacts, used when creating model version.
    */
    'source'?: string;
    /**
    * Run ID used when creating model version.
    */
    'runId'?: string;
    /**
    * Current status of model version.
    */
    'status'?: model.ModelVersionStatus;
    /**
    * Details on current status, if it is pending or failed.
    */
    'statusMessage'?: string;
    /**
    * Tags of model version.
    */
    'tags'?: Array<model.ModelVersionTag>;
    /**
    * Direct link to the run that generated this version.
    */
    'runLink'?: string;
    /**
    * Aliases pointing to this version.
    */
    'aliases'?: Array<string>;
    /**
    * Model ID for model version that is used to link the registered model to the source logged model.
    */
    'modelId'?: string;
    /**
    * Metrics logged for the model.
    */
    'modelMetrics'?: Array<model.ModelMetric>;
    /**
    * Parameters logged for the model.
    */
    'modelParams'?: Array<model.ModelParam>;
    'deploymentJobState'?: model.ModelVersionDeploymentJobState;

}

export namespace ModelVersion {



















    export function getJsonObj(obj: ModelVersion): object {
        const jsonObj = {...obj, ...{
            


                'creation_timestamp': obj.creationTimestamp,

                'last_updated_timestamp': obj.lastUpdatedTimestamp,

                'user_id': obj.userId,

                'current_stage': obj.currentStage,



                'run_id': obj.runId,


                'status_message': obj.statusMessage,

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ModelVersionTag.getJsonObj(item)})
                
                 : undefined,
                'run_link': obj.runLink,


                'model_id': obj.modelId,

                'model_metrics': obj.modelMetrics ?
                
                obj.modelMetrics.map((item)=>{return model.ModelMetric.getJsonObj(item)})
                
                 : undefined,
                'model_params': obj.modelParams ?
                
                obj.modelParams.map((item)=>{return model.ModelParam.getJsonObj(item)})
                
                 : undefined,
                'deployment_job_state': obj.deploymentJobState ?
                
                
                model.ModelVersionDeploymentJobState.getJsonObj(obj.deploymentJobState) : undefined,
        }};

        delete (jsonObj as Partial<ModelVersion>).creationTimestamp;delete (jsonObj as Partial<ModelVersion>).lastUpdatedTimestamp;delete (jsonObj as Partial<ModelVersion>).userId;delete (jsonObj as Partial<ModelVersion>).currentStage;delete (jsonObj as Partial<ModelVersion>).runId;delete (jsonObj as Partial<ModelVersion>).statusMessage;delete (jsonObj as Partial<ModelVersion>).runLink;delete (jsonObj as Partial<ModelVersion>).modelId;delete (jsonObj as Partial<ModelVersion>).modelMetrics;delete (jsonObj as Partial<ModelVersion>).modelParams;delete (jsonObj as Partial<ModelVersion>).deploymentJobState;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelVersion): object {
        const jsonObj = {...obj, ...{
            


                'creationTimestamp': (obj as any)["creation_timestamp"],

                'lastUpdatedTimestamp': (obj as any)["last_updated_timestamp"],

                'userId': (obj as any)["user_id"],

                'currentStage': (obj as any)["current_stage"],



                'runId': (obj as any)["run_id"],


                'statusMessage': (obj as any)["status_message"],

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ModelVersionTag.getDeserializedJsonObj(item)})
                
                 : undefined,
                'runLink': (obj as any)["run_link"],


                'modelId': (obj as any)["model_id"],

                    'modelMetrics': (obj as any)["model_metrics"] ?
                
                (obj as any)["model_metrics"].map((item: any)=>{return model.ModelMetric.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'modelParams': (obj as any)["model_params"] ?
                
                (obj as any)["model_params"].map((item: any)=>{return model.ModelParam.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'deploymentJobState': (obj as any)["deployment_job_state"] ?
                
                
                model.ModelVersionDeploymentJobState.getDeserializedJsonObj((obj as any)["deployment_job_state"]) : undefined,
         }};

        delete (jsonObj as any)["creation_timestamp"];delete (jsonObj as any)["last_updated_timestamp"];delete (jsonObj as any)["user_id"];delete (jsonObj as any)["current_stage"];delete (jsonObj as any)["run_id"];delete (jsonObj as any)["status_message"];delete (jsonObj as any)["run_link"];delete (jsonObj as any)["model_id"];delete (jsonObj as any)["model_metrics"];delete (jsonObj as any)["model_params"];delete (jsonObj as any)["deployment_job_state"];
        
        return jsonObj;
    }
}
