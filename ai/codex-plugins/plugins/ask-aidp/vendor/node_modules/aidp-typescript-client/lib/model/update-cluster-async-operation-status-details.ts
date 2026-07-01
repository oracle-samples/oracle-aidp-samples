// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update cluster async operation status.
*/
export interface UpdateClusterAsyncOperationStatusDetails {
    /**
    * Descriptive message of the current state.
    */
    'stateMessage'?: string;
    /**
    * Common lifecycle states for resources in a compute cluster.
* ACCEPTED        - The resource create request has been accepted.
* CREATING        - The resource is being created and might not be usable until the entire metadata is defined.
* ACTIVE          - The resource is valid and available for access.
* DELETING        - The resource is being deleted, and might require a deep clean of any children.
* DELETED         - The resource has been deleted, and isn't available.
* FAILED          - The resource is in a failed state due to validation or other errors.
* STOPPING        - The resource is being stopped.
* STOPPED         - The resource has been stopped.
* UPDATING        - The resource is being updated and might not be usable until all changes are commited.
* STARTING        - The resource is being started.
* RESTARTING      - The resource is being restarted.
* 
    */
    'state': UpdateClusterAsyncOperationStatusDetails.State;
    /**
    * External work-request-id if applicable.
    */
    'externalId'?: string;
    /**
    * Metrics for the cluster operation.
    */
    'metrics'?: { [key: string]: string; };
    /**
    * Properties of operation on cluster.
    */
    'properties'?: { [key: string]: any; };

}

export namespace UpdateClusterAsyncOperationStatusDetails {


    export enum State {
    
    Accepted = "ACCEPTED",
    Creating = "CREATING",
    Active = "ACTIVE",
    Deleting = "DELETING",
    Deleted = "DELETED",
    Failed = "FAILED",
    Stopping = "STOPPING",
    Stopped = "STOPPED",
    Updating = "UPDATING",
    Restarting = "RESTARTING",
    Starting = "STARTING",
    NetworkConfigurationAttachInProgress = "NETWORK_CONFIGURATION_ATTACH_IN_PROGRESS",
    NetworkConfigurationAttachSuccessful = "NETWORK_CONFIGURATION_ATTACH_SUCCESSFUL",
    NetworkConfigurationAttachFailed = "NETWORK_CONFIGURATION_ATTACH_FAILED",
    NetworkConfigurationDetachInProgress = "NETWORK_CONFIGURATION_DETACH_IN_PROGRESS",
    NetworkConfigurationDetachSuccessful = "NETWORK_CONFIGURATION_DETACH_SUCCESSFUL",
    NetworkConfigurationDetachFailed = "NETWORK_CONFIGURATION_DETACH_FAILED"

}





    export function getJsonObj(obj: UpdateClusterAsyncOperationStatusDetails): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateClusterAsyncOperationStatusDetails): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
