// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information of view in the schema.
*/
export interface ViewSummary {
    /**
    * The fully qualified name of the view in the format <catalog_name>.<schema_name>.<view_name>
    */
    'key'?: string;
    /**
    * A user-friendly name. Has to be unique within the scope of the schema and is changeable.
    */
    'displayName'?: string;
    /**
    * Denotes whether the view is temporary or permanent.
    */
    'isTemporary'?: boolean;
    /**
    * The date and time the View was created, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated'?: Date;
    /**
    * The date and time the View was updated, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeUpdated'?: Date;
    /**
    * The ID of the user/principal who created the view.
    */
    'createdBy'?: string;
    /**
    * The ID of the user who last updated the view.
    */
    'updatedBy'?: string;
    /**
    * The state of the Table.
    */
    'lifecycleState'?: model.ViewLifecycleState;

}

export namespace ViewSummary {









    export function getJsonObj(obj: ViewSummary): object {
        const jsonObj = {...obj, ...{
            








        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ViewSummary): object {
        const jsonObj = {...obj, ...{
            








         }};

        
        
        return jsonObj;
    }
}
