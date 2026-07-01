// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a schema permission.
*/
export interface SchemaPermissionSummary {
    /**
    * OCID of user/group and name in case of role.
    */
    'grantee': string;
    /**
    * Simplified name of the grantee.
    */
    'granteeName'?: string;
    /**
    * Type of grantee.
    */
    'granteeType': model.GranteeType;
    /**
    * Selected permissions for a schema.
    */
    'granteePermissions': Array<SchemaPermissionSummary.GranteePermissions>;
    /**
    * Permission listed is inherited or not from object higher up in hierarchy.
    */
    'isInherited'?: boolean;
    /**
    * Name of the object to which this permission belongs. This would be the name of schema if permission is not inherited or name of object higher up in hierarchy if permission is inherited.
    */
    'resourceName'?: string;

}

export namespace SchemaPermissionSummary {




    export enum GranteePermissions {
    
    Select = "SELECT",
    Manage = "MANAGE",
    Write = "WRITE",
    CreateView = "CREATE_VIEW",
    CreateVolume = "CREATE_VOLUME",
    CreateTable = "CREATE_TABLE",
    CreateKnowledgeBase = "CREATE_KNOWLEDGE_BASE",
    Admin = "ADMIN",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}




    export function getJsonObj(obj: SchemaPermissionSummary): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SchemaPermissionSummary): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
