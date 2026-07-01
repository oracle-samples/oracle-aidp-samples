// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Notifier API during cluster patching.
*/
export interface NotifyClusterEventHandlerDetails {
    /**
    * A unique name for the job cluster.
    */
    'clusterName': string;
    /**
    * Phase
    */
    'phase'?: NotifyClusterEventHandlerDetails.Phase;
    /**
    * State of cluster.
    */
    'state'?: NotifyClusterEventHandlerDetails.State;
    'clusterEvent'?: model.ClusterStateEvent| model.ClusterPatchEvent| model.ClusterExecutionContextAvailabilityEvent;

}

export namespace NotifyClusterEventHandlerDetails {


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



    export function getJsonObj(obj: NotifyClusterEventHandlerDetails): object {
        const jsonObj = {...obj, ...{
            



                'clusterEvent': obj.clusterEvent ?
                
                
                model.ClusterEvent.getJsonObj(obj.clusterEvent) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: NotifyClusterEventHandlerDetails): object {
        const jsonObj = {...obj, ...{
            



                    'clusterEvent': obj.clusterEvent ?
                
                
                model.ClusterEvent.getDeserializedJsonObj(obj.clusterEvent) : undefined,
         }};

        
        
        return jsonObj;
    }
}
