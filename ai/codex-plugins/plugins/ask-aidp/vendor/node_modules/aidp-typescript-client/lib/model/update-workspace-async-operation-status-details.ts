// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update workspace async operation status.
*/
export interface UpdateWorkspaceAsyncOperationStatusDetails {
    /**
    * A descriptive message of the current state.
    */
    'stateMessage'?: string;
    /**
    * Common lifecycle states for resources in a Compute Cluster:
* NETWORK_CONFIGURATION_CREATED       - The network configuration has been created.
* NETWORK_CONFIGURATION_UPDATED       - The network configuration has been updated.
* NETWORK_CONFIGURATION_DELETED       - The network configuration has been deleted.
* NETWORK_CONFIGURATION_FAILED        - The network configuration has been failed.
* 
    */
    'state': UpdateWorkspaceAsyncOperationStatusDetails.State;
    /**
    * The external work-request-id if applicable.
    */
    'externalId'?: string;
    /**
    * The properties of operation on workspace.
    */
    'properties'?: { [key: string]: any; };

}

export namespace UpdateWorkspaceAsyncOperationStatusDetails {


    export enum State {
    
    NetworkConfigurationCreated = "NETWORK_CONFIGURATION_CREATED",
    NetworkConfigurationUpdated = "NETWORK_CONFIGURATION_UPDATED",
    NetworkConfigurationDeleted = "NETWORK_CONFIGURATION_DELETED",
    NetworkConfigurationFailed = "NETWORK_CONFIGURATION_FAILED"

}




    export function getJsonObj(obj: UpdateWorkspaceAsyncOperationStatusDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateWorkspaceAsyncOperationStatusDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
