// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A WorkspaceObject is a file or folder belonging to an AI Data Platform Workbench workspace.
* To use any of the API operations, you must be authorized in an IAM policy. If you're not authorized, talk to
* an administrator. If you're an administrator who needs to write policies to give users access, see
* <a href=\"https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html\" target=\"_blank\" rel=\"noopener noreferrer\">IAM Policies for Oracle AI Data Platform Workbench</a>.
* 
*/
export interface WorkspaceObjectDetails {
    /**
    * The fully qualified path of the workspace object.
* Example: /Shared/Folder1/Notebook1.ipynb
* 
    */
    'path': string;
    /**
    * The key of the Workspace Object.
    */
    'key'?: string;
    /**
    * The name of the Workspace Object. This will be the name of the file/folder in the workspace.
* Example: Notebook1.ipynb, Folder1
* 
    */
    'displayName': string;
    /**
    * The date and time the workspace object was created, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated': Date;
    /**
    * The date and time the workspace object was created, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeUpdated'?: Date;
    /**
    * The type of the workspace object.
    */
    'type': WorkspaceObjectDetails.Type;
    /**
    * The description for the file and folder.
    */
    'description'?: string;
    /**
    * System tags for this resource. Each key is predefined and scoped to a namespace.
* <p>
Example: {@code {\"orcl-cloud\": {\"free-tier-retained\": \"true\"}}}
* 
    */
    'systemTags'?: { [key: string]: { [key: string]: any; }; };

}

export namespace WorkspaceObjectDetails {






    export enum Type {
    
    Notebook = "NOTEBOOK",
    Job = "JOB",
    LakeFlow = "LAKE_FLOW",
    AgentLakeFlow = "AGENT_LAKE_FLOW",
    GeneratedArtifact = "GENERATED_ARTIFACT",
    Library = "LIBRARY",
    File = "FILE",
    Folder = "FOLDER",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}




    export function getJsonObj(obj: WorkspaceObjectDetails): object {
        const jsonObj = {...obj, ...{
            








        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceObjectDetails): object {
        const jsonObj = {...obj, ...{
            








         }};

        
        
        return jsonObj;
    }
}
