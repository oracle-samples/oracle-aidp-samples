// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the registered model
*/
export interface RegisteredModel {
    /**
    * Unique name for the model.
    */
    'name'?: string;
    /**
    * Timestamp in milliseconds when the model was created. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'creationTimestamp'?: number;
    /**
    * Timestamp in milliseconds when metadata for the model was last updated. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'lastUpdatedTimestamp'?: number;
    /**
    * Description of the registered model.
    */
    'description'?: string;
    /**
    * Collection of latest model versions for each stage. Only contains models with current READY status.
    */
    'latestVersions'?: Array<model.ModelVersion>;
    /**
    * Aliases pointing to model versions associated with this registered_model.
    */
    'aliases'?: Array<model.RegisteredModelAlias>;
    /**
    * Deployment job ID.
    */
    'deploymentJobId'?: string;
    /**
    * Job state.
    */
    'deploymentJobState'?: model.DeploymentJobState;
    /**
    * Tags for the registered model.
    */
    'tags'?: Array<model.RegisteredModelTag>;

}

export namespace RegisteredModel {










    export function getJsonObj(obj: RegisteredModel): object {
        const jsonObj = {...obj, ...{
            

                'creation_timestamp': obj.creationTimestamp,

                'last_updated_timestamp': obj.lastUpdatedTimestamp,


                'latest_versions': obj.latestVersions ?
                
                obj.latestVersions.map((item)=>{return model.ModelVersion.getJsonObj(item)})
                
                 : undefined,
                'aliases': obj.aliases ?
                
                obj.aliases.map((item)=>{return model.RegisteredModelAlias.getJsonObj(item)})
                
                 : undefined,
                'deployment_job_id': obj.deploymentJobId,

                'deployment_job_state': obj.deploymentJobState,

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.RegisteredModelTag.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<RegisteredModel>).creationTimestamp;delete (jsonObj as Partial<RegisteredModel>).lastUpdatedTimestamp;delete (jsonObj as Partial<RegisteredModel>).latestVersions;delete (jsonObj as Partial<RegisteredModel>).deploymentJobId;delete (jsonObj as Partial<RegisteredModel>).deploymentJobState;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RegisteredModel): object {
        const jsonObj = {...obj, ...{
            

                'creationTimestamp': (obj as any)["creation_timestamp"],

                'lastUpdatedTimestamp': (obj as any)["last_updated_timestamp"],


                    'latestVersions': (obj as any)["latest_versions"] ?
                
                (obj as any)["latest_versions"].map((item: any)=>{return model.ModelVersion.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'aliases': obj.aliases ?
                
                obj.aliases.map((item)=>{return model.RegisteredModelAlias.getDeserializedJsonObj(item)})
                
                 : undefined,
                'deploymentJobId': (obj as any)["deployment_job_id"],

                'deploymentJobState': (obj as any)["deployment_job_state"],

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.RegisteredModelTag.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["creation_timestamp"];delete (jsonObj as any)["last_updated_timestamp"];delete (jsonObj as any)["latest_versions"];delete (jsonObj as any)["deployment_job_id"];delete (jsonObj as any)["deployment_job_state"];
        
        return jsonObj;
    }
}
