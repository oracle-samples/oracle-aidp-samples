// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the logged model.
*/
export interface LoggedModel {
    'info'?: model.LoggedModelInfo;
    /**
    * Details of the logged model data.
    */
    'data'?: any;

}

export namespace LoggedModel {



    export function getJsonObj(obj: LoggedModel): object {
        const jsonObj = {...obj, ...{
            
                'info': obj.info ?
                
                
                model.LoggedModelInfo.getJsonObj(obj.info) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LoggedModel): object {
        const jsonObj = {...obj, ...{
            
                    'info': obj.info ?
                
                
                model.LoggedModelInfo.getDeserializedJsonObj(obj.info) : undefined,

         }};

        
        
        return jsonObj;
    }
}
