// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a RegisteredModel.
*/
export interface UpdateRegisteredModelDetails {
    /**
    * Registered model unique name.
    */
    'name': string;
    /**
    * Description for the registered model.
    */
    'description'?: string;
    /**
    * Deployment job id for this model.
    */
    'deploymentJobId'?: string;

}

export namespace UpdateRegisteredModelDetails {




    export function getJsonObj(obj: UpdateRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            


                'deployment_job_id': obj.deploymentJobId,

        }};

        delete (jsonObj as Partial<UpdateRegisteredModelDetails>).deploymentJobId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            


                'deploymentJobId': (obj as any)["deployment_job_id"],

         }};

        delete (jsonObj as any)["deployment_job_id"];
        
        return jsonObj;
    }
}
