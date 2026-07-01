// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details for updating a view.
*/
export interface ViewUpdateDetails {
    /**
    * A user-friendly name. Has to be unique within the scope of the schema and is changeable.
    */
    'displayName'?: string;
    /**
    * The description of the view.
    */
    'description'?: string;
    /**
    * The Query used to create the view.
    */
    'viewText'?: string;
    /**
    * Columns for view.
    */
    'viewFields'?: Array<model.ViewFieldDetails>;
    /**
    * View Properties.
    */
    'addViewProperties'?: Array<model.ViewProperty>;
    /**
    * View Properties.
    */
    'dropViewProperties'?: Array<model.ViewProperty>;

}

export namespace ViewUpdateDetails {







    export function getJsonObj(obj: ViewUpdateDetails): object {
        const jsonObj = {...obj, ...{
            



                'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'addViewProperties': obj.addViewProperties ?
                
                obj.addViewProperties.map((item)=>{return model.ViewProperty.getJsonObj(item)})
                
                 : undefined,
                'dropViewProperties': obj.dropViewProperties ?
                
                obj.dropViewProperties.map((item)=>{return model.ViewProperty.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ViewUpdateDetails): object {
        const jsonObj = {...obj, ...{
            



                    'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'addViewProperties': obj.addViewProperties ?
                
                obj.addViewProperties.map((item)=>{return model.ViewProperty.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'dropViewProperties': obj.dropViewProperties ?
                
                obj.dropViewProperties.map((item)=>{return model.ViewProperty.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
