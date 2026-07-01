// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a Workspace.
*/
export interface WorkspaceSummary {
    /**
    * The key of the AI Data Platform Workbench workspace.
    */
    'key': string;
    /**
    * A user-friendly name that has to be unique in a AI Data Platform Workbench instance.
    */
    'displayName': string;
    /**
    * Workspace type. Type is DEFAULT for workspace created at AI Data Platform Workbench creation, type is USER for workspace created by AI Data Platform Workbench user.
    */
    'type': WorkspaceSummary.Type;
    /**
    * Description of the workspace.
    */
    'description'?: string;
    /**
    * The date and time the AI Data Platform Workbench workspace was created, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated': Date;
    /**
    * The date and time the AI Data Platform Workbench workspace was updated, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeUpdated'?: Date;
    /**
    * The current state of the AI Data Platform Workbench workspace.
    */
    'lifecycleState': string;
    /**
    * A message that describes the current state of the workspace in more detail. For example,
* can be used to provide actionable information for a resource in the Failed state.
* 
    */
    'lifecycleDetails'?: string;
    /**
    * System tags for this resource. Each key is predefined and scoped to a namespace.
* <p>
Example: {@code {\"orcl-cloud\": {\"free-tier-retained\": \"true\"}}}
* 
    */
    'systemTags'?: { [key: string]: { [key: string]: any; }; };
    /**
    * OCID of the user who created this record.
    */
    'createdBy'?: string;
    /**
    * Name of the user who created this record.
    */
    'createdByName'?: string;
    /**
    * OCID of the user who updated this record.
    */
    'updatedBy'?: string;
    /**
    * Name of the user who updated this record.
    */
    'updatedByName'?: string;
    /**
    * The key of the catalog to be used as the default catalog for this workspace.
* A default catalog in the workspace will allow users to use that
* catalog without the need to refer it in the notebook. For example, if default catalog is iCat1, and it has
* schema1 and table1, you can refer to the table in a notebook using: schema1.table1.
* 
    */
    'defaultCatalogKey'?: string;
    /**
    * A flag to display whether workspace is private network enabled or not.
    */
    'isPrivateNetworkEnabled'?: boolean;
    /**
    * The name of the AIC user schema if created.
    */
    'aicUserSchemaName'?: string;

}

export namespace WorkspaceSummary {



    export enum Type {
    
    Default = "DEFAULT",
    User = "USER",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}















    export function getJsonObj(obj: WorkspaceSummary): object {
        const jsonObj = {...obj, ...{
            
















        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceSummary): object {
        const jsonObj = {...obj, ...{
            
















         }};

        
        
        return jsonObj;
    }
}
