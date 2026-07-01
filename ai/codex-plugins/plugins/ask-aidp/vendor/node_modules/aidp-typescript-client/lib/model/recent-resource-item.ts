// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The list of the recent resources accessed by a user
* 
*/
export interface RecentResourceItem {
    /**
    * Key of the workspace. Null in the case of a global resource.
    */
    'workspaceKey'?: string;
    /**
    * Type of the resource.
    */
    'resourceType'?: RecentResourceItem.ResourceType;
    /**
    * Unique identifier of the resource or path.
    */
    'resourceId'?: string;
    /**
    * Resource name associated with the resourceId field.
    */
    'resourceName'?: string;
    /**
    * Timestamp of when the resource was created, read, or updated.
    */
    'timeAccessed'?: Date;

}

export namespace RecentResourceItem {


    export enum ResourceType {
    
    Notebook = "NOTEBOOK",
    File = "FILE",
    Catalog = "CATALOG",
    Schema = "SCHEMA",
    Table = "TABLE",
    Volume = "VOLUME",
    Job = "JOB",
    JobRun = "JOB_RUN",
    LakeFlow = "LAKE_FLOW",
    AgentLakeFlow = "AGENT_LAKE_FLOW",
    Cluster = "CLUSTER",
    AiCompute = "AI_COMPUTE",
    Folder = "FOLDER",
    VolumeDir = "VOLUME_DIR",
    VolumeFile = "VOLUME_FILE",
    View = "VIEW",
    Share = "SHARE",
    Recipient = "RECIPIENT",
    Extractor = "EXTRACTOR",
    AgentFlow = "AGENT_FLOW"

}





    export function getJsonObj(obj: RecentResourceItem): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RecentResourceItem): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
