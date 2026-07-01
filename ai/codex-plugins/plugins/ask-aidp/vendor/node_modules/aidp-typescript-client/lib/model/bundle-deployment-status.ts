// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary of the most recent completed bundle deployment.
* 
*/
export interface BundleDeploymentStatus {
    /**
    * Overall status of the last deployment.
    */
    'status': BundleDeploymentStatus.Status;
    /**
    * The deployment start time
    */
    'timeStarted': Date;
    /**
    * The deployment end time
    */
    'timeCompleted': Date;
    /**
    * Optional summary message for the last deployment.
    */
    'message'?: string;
    /**
    * List of resources from the last deployment.
    */
    'resources'?: Array<model.BundleDeployedResource>;

}

export namespace BundleDeploymentStatus {

    export enum Status {
    
    Succeeded = "SUCCEEDED",
    Failed = "FAILED",
    InProgress = "IN_PROGRESS",
    NotDeployed = "NOT_DEPLOYED",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}






    export function getJsonObj(obj: BundleDeploymentStatus): object {
        const jsonObj = {...obj, ...{
            




                'resources': obj.resources ?
                
                obj.resources.map((item)=>{return model.BundleDeployedResource.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: BundleDeploymentStatus): object {
        const jsonObj = {...obj, ...{
            




                    'resources': obj.resources ?
                
                obj.resources.map((item)=>{return model.BundleDeployedResource.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
