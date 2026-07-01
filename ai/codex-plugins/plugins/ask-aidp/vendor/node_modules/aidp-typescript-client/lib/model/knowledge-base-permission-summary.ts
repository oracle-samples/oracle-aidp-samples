// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about a Knowledge Base Permission.
*/
export interface KnowledgeBasePermissionSummary {
    /**
    * The ocid of user/group and name in case of role.
    */
    'grantee': string;
    /**
    * The simplified name of the grantee.
    */
    'granteeName'?: string;
    /**
    * The type of grantee.
    */
    'granteeType': model.GranteeType;
    /**
    * The selected permissions for a Knowledge Base.
    */
    'granteePermissions': Array<KnowledgeBasePermissionSummary.GranteePermissions>;
    /**
    * The permission listed is inherited or not from object up in hierarchy.
    */
    'isInherited'?: boolean;
    /**
    * name of the object to which this permission belong to. This would be the name of table if permission is not inherited or name of object up in hierarchy if permission is inherited.
    */
    'resourceName'?: string;

}

export namespace KnowledgeBasePermissionSummary {




    export enum GranteePermissions {
    
    Write = "WRITE",
    Admin = "ADMIN",
    Select = "SELECT",
    Manage = "MANAGE"

}




    export function getJsonObj(obj: KnowledgeBasePermissionSummary): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBasePermissionSummary): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
