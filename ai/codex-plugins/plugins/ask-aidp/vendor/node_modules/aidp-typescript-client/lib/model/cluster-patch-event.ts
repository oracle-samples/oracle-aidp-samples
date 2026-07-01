// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about the cluster patch event.
*/
export interface ClusterPatchEvent extends model.ClusterEvent {
    /**
    * Phase
    */
    'phase'?: ClusterPatchEvent.Phase;
    /**
    * State of cluster.
    */
    'state'?: ClusterPatchEvent.State;

   "type": string;
}

export namespace ClusterPatchEvent {

    export enum Phase {
    
    Started = "STARTED",
    Completed = "COMPLETED"

}


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


    export function getJsonObj(obj: ClusterPatchEvent, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterEvent.getJsonObj(obj) as ClusterPatchEvent, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const type = 'CLUSTER_PATCH_EVENT';
    export function getDeserializedJsonObj(obj: ClusterPatchEvent, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterEvent.getDeserializedJsonObj(obj) as ClusterPatchEvent, ...{
            


         }};

        
        
        return jsonObj;
    }
}
