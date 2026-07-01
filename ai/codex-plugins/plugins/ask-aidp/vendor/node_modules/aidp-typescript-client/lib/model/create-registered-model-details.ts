// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a registered model.
*/
export interface CreateRegisteredModelDetails {
    /**
    * Register models under this name.
    */
    'name': string;
    /**
    * Tags for the registered model.
    */
    'tags'?: Array<model.RegisteredModelTag>;
    /**
    * Description for the registered model.
    */
    'description'?: string;
    /**
    * Deployment job ID for this model.
    */
    'deploymentJobId'?: string;

}

export namespace CreateRegisteredModelDetails {





    export function getJsonObj(obj: CreateRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            

                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.RegisteredModelTag.getJsonObj(item)})
                
                 : undefined,

                'deployment_job_id': obj.deploymentJobId,

        }};

        delete (jsonObj as Partial<CreateRegisteredModelDetails>).deploymentJobId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            

                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.RegisteredModelTag.getDeserializedJsonObj(item)})
                
                 : undefined,

                'deploymentJobId': (obj as any)["deployment_job_id"],

         }};

        delete (jsonObj as any)["deployment_job_id"];
        
        return jsonObj;
    }
}
