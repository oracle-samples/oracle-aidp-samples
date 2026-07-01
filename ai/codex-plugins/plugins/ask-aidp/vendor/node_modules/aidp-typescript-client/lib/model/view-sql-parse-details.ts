// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Parsed view select query post validation.
*/
export interface ViewSqlParseDetails {
    /**
    * Flag to indicate whether given view SQL is valid or not.
    */
    'isQueryValid': boolean;
    /**
    * Error message if given view SQL is not valid.
    */
    'queryParseErrorMessage'?: string;
    /**
    * Columns for view.
    */
    'viewFields'?: Array<model.ViewFieldDetails>;

}

export namespace ViewSqlParseDetails {




    export function getJsonObj(obj: ViewSqlParseDetails): object {
        const jsonObj = {...obj, ...{
            


                'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ViewSqlParseDetails): object {
        const jsonObj = {...obj, ...{
            


                    'viewFields': obj.viewFields ?
                
                obj.viewFields.map((item)=>{return model.ViewFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
