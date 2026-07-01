// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a view.
*/
export interface CreateViewDetails {
    /**
    * A user-friendly name. Has to be unique within the scope of the schema and is changeable.
    */
    'displayName': string;
    /**
    * The name of the catalog to which this view belongs.
    */
    'catalogKey': string;
    /**
    * The name of the Schema to which this view belongs.
    */
    'schemaKey': string;
    /**
    * The description of the view.
    */
    'description'?: string;
    /**
    * The Query used to create the view.
    */
    'viewText': string;
    /**
    * View Properties.
    */
    'viewProperties'?: Array<model.ViewProperty>;
    /**
    * Columns for view.
    */
    'viewFields'?: Array<model.ViewFieldDetails>;

}

export namespace CreateViewDetails {








    export function getJsonObj(obj: CreateViewDetails): object {
        const jsonObj = {...obj, ...{
            





                'viewProperties': obj.viewProperties ?
                
                obj.viewProperties.map((item)=>{return model.ViewProperty.getJsonObj(item)})
                
                 : undefined,
                'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateViewDetails): object {
        const jsonObj = {...obj, ...{
            





                    'viewProperties': obj.viewProperties ?
                
                obj.viewProperties.map((item)=>{return model.ViewProperty.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
