// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about the cluster state event. Workflow will use it to obtain the terminal state.
*/
export interface ClusterStateEvent extends model.ClusterEvent {
    /**
    * State of cluster.
    */
    'state'?: ClusterStateEvent.State;

   "type": string;
}

export namespace ClusterStateEvent {

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


    export function getJsonObj(obj: ClusterStateEvent, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterEvent.getJsonObj(obj) as ClusterStateEvent, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'CLUSTER_STATE_EVENT';
    export function getDeserializedJsonObj(obj: ClusterStateEvent, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterEvent.getDeserializedJsonObj(obj) as ClusterStateEvent, ...{
            

         }};

        
        
        return jsonObj;
    }
}
