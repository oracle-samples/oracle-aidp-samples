// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for creating a RegisteredModel
*/
export interface CreateRegisteredModelResponseDetails {
    'registeredModel': model.RegisteredModel;

}

export namespace CreateRegisteredModelResponseDetails {


    export function getJsonObj(obj: CreateRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'registered_model': obj.registeredModel ?
                
                
                model.RegisteredModel.getJsonObj(obj.registeredModel) : undefined,
        }};

        delete (jsonObj as Partial<CreateRegisteredModelResponseDetails>).registeredModel;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'registeredModel': (obj as any)["registered_model"] ?
                
                
                model.RegisteredModel.getDeserializedJsonObj((obj as any)["registered_model"]) : undefined,
         }};

        delete (jsonObj as any)["registered_model"];
        
        return jsonObj;
    }
}
