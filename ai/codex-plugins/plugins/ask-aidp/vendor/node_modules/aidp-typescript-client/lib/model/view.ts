// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about the view in the schema.
*/
export interface View {
    /**
    * The fully qualified name of the view in the format <catalog_name>.<schema_name>.<view_name>
    */
    'key'?: string;
    /**
    * A user-friendly name. Has to be unique within the scope of the schema and is changeable.
    */
    'displayName': string;
    /**
    * The name of the catalog to which this view belongs.
    */
    'catalogKey'?: string;
    /**
    * The name of the Schema to which this view belongs.
    */
    'schemaKey'?: string;
    /**
    * The Query used to create the view.
    */
    'viewText'?: string;
    /**
    * The description of the view.
    */
    'description'?: string;
    /**
    * Columns for view.
    */
    'viewFields'?: Array<model.ViewFieldDetails>;
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
    * View Properties.
    */
    'viewProperties'?: Array<model.ViewProperty>;
    /**
    * The state of the view.
    */
    'lifecycleState'?: model.ViewLifecycleState;
    /**
    * A message describing the current state in more detail. For example, it can be used to provide actionable information for a resource in Failed state.
    */
    'lifecycleStateDetails'?: string;

}

export namespace View {















    export function getJsonObj(obj: View): object {
        const jsonObj = {...obj, ...{
            






                'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getJsonObj(item)})
                
                 : undefined,




                'viewProperties': obj.viewProperties ?
                
                obj.viewProperties.map((item)=>{return model.ViewProperty.getJsonObj(item)})
                
                 : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: View): object {
        const jsonObj = {...obj, ...{
            






                    'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,




                    'viewProperties': obj.viewProperties ?
                
                obj.viewProperties.map((item)=>{return model.ViewProperty.getDeserializedJsonObj(item)})
                
                 : undefined,


         }};

        
        
        return jsonObj;
    }
}
