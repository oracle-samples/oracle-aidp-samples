// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The summary of the role.
*/
export interface RoleSummary {
    /**
    * A unique key for the role summary. It cannot be changed.
    */
    'key': string;
    /**
    * The role summary name. It can be changed.
    */
    'displayName': string;
    /**
    * Type of role.
    */
    'roleType'?: model.RoleType;
    /**
    * The time the role summary was created. An RFC3339 formatted datetime string.
    */
    'timeCreated'?: Date;
    /**
    * The time the role summary was updated. An RFC3339 formatted datetime string.
    */
    'timeUpdated'?: Date;
    /**
    * The role summary is assigned to the current user or a group that the user is part of.
    */
    'isAssigned'?: boolean;
    /**
    * The user name of the user/principal who created the role.
    */
    'createdBy'?: string;
    /**
    * The user name of the user/principal who updated the role.
    */
    'updatedBy'?: string;
    /**
    * The current state of the role summary.
    */
    'lifecycleState'?: string;
    /**
    * A message describing the current state in more detail. For example, it can be used to provide actionable information for a resource in Failed state.
    */
    'lifecycleDetails'?: string;
    /**
    * The description of the role summary.
    */
    'description'?: string;

}

export namespace RoleSummary {












    export function getJsonObj(obj: RoleSummary): object {
        const jsonObj = {...obj, ...{
            











        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RoleSummary): object {
        const jsonObj = {...obj, ...{
            











         }};

        
        
        return jsonObj;
    }
}
