// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for getting a RegisteredModel
*/
export interface GetRegisteredModelResponseDetails {
    'registeredModel': model.RegisteredModel;

}

export namespace GetRegisteredModelResponseDetails {


    export function getJsonObj(obj: GetRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'registered_model': obj.registeredModel ?
                
                
                model.RegisteredModel.getJsonObj(obj.registeredModel) : undefined,
        }};

        delete (jsonObj as Partial<GetRegisteredModelResponseDetails>).registeredModel;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GetRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'registeredModel': (obj as any)["registered_model"] ?
                
                
                model.RegisteredModel.getDeserializedJsonObj((obj as any)["registered_model"]) : undefined,
         }};

        delete (jsonObj as any)["registered_model"];
        
        return jsonObj;
    }
}
