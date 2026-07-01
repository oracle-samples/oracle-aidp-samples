// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a cluster library.
*/
export interface ClusterLibrarySummary {
    /**
    * Date and time the library was installed.
    */
    'timeCreated'?: Date;
    /**
    * Date and time the library was updated.
    */
    'timeUpdated'?: Date;
    /**
    * Additional context or detail about the current state of the library, especially useful when the status is {@code FAILED}, {@code SKIPPED}, or requires user intervention.
* This message can contain information such as the reason for failure, the step where the installation failed, or other diagnostic messages.
* 
    */
    'stateMessage'?: string;
    /**
    * Status of the library installed on the cluster.
    */
    'status'?: ClusterLibrarySummary.Status;

   "type": string;
}

export namespace ClusterLibrarySummary {




    export enum Status {
    
    Pending = "PENDING",
    Resolving = "RESOLVING",
    Installing = "INSTALLING",
    Installed = "INSTALLED",
    Failed = "FAILED",
    InstallOnRestart = "INSTALL_ON_RESTART",
    UninstallOnRestart = "UNINSTALL_ON_RESTART",
    Skipped = "SKIPPED",
    Deleted = "DELETED",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: ClusterLibrarySummary): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "WORKSPACE_FILE":
                    return model.WorkspaceFileClusterLibrarySummary.getJsonObj(<model.WorkspaceFileClusterLibrarySummary>(<object>jsonObj), true);
                case "VOLUME_FILE":
                    return model.VolumeFileClusterLibrarySummary.getJsonObj(<model.VolumeFileClusterLibrarySummary>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterLibrarySummary): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "WORKSPACE_FILE":
                    return model.WorkspaceFileClusterLibrarySummary.getDeserializedJsonObj(<model.WorkspaceFileClusterLibrarySummary>(<object>jsonObj), true);
                case "VOLUME_FILE":
                    return model.VolumeFileClusterLibrarySummary.getDeserializedJsonObj(<model.VolumeFileClusterLibrarySummary>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
