// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a cluster library to install or uninstall.
*/
export interface ClusterLibraryDetails {
    /**
    * Library type.
    */
    'type'?: ClusterLibraryDetails.Type;

   "operation": string;
}

export namespace ClusterLibraryDetails {

    export enum Type {
    
    WorkspaceFile = "WORKSPACE_FILE",
    VolumeFile = "VOLUME_FILE"

}


    export function getJsonObj(obj: ClusterLibraryDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        if (obj && "operation" in obj && obj.operation) {
            switch (obj.operation) {
                case "UNINSTALL":
                    return model.UninstallClusterLibraryDetails.getJsonObj(<model.UninstallClusterLibraryDetails>(<object>jsonObj), true);
                case "INSTALL":
                    return model.InstallClusterLibraryDetails.getJsonObj(<model.InstallClusterLibraryDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.operation}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterLibraryDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        if (obj && "operation" in obj && obj.operation) {
            switch (obj.operation) {
                case "UNINSTALL":
                    return model.UninstallClusterLibraryDetails.getDeserializedJsonObj(<model.UninstallClusterLibraryDetails>(<object>jsonObj), true);
                case "INSTALL":
                    return model.InstallClusterLibraryDetails.getDeserializedJsonObj(<model.InstallClusterLibraryDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.operation}`)
        }
        }
        return jsonObj;
    }
}
