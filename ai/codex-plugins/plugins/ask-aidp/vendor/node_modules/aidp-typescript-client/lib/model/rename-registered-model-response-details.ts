// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for renaming a registered model.
*/
export interface RenameRegisteredModelResponseDetails {
    'registeredModel': model.RegisteredModel;

}

export namespace RenameRegisteredModelResponseDetails {


    export function getJsonObj(obj: RenameRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'registered_model': obj.registeredModel ?
                
                
                model.RegisteredModel.getJsonObj(obj.registeredModel) : undefined,
        }};

        delete (jsonObj as Partial<RenameRegisteredModelResponseDetails>).registeredModel;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RenameRegisteredModelResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'registeredModel': (obj as any)["registered_model"] ?
                
                
                model.RegisteredModel.getDeserializedJsonObj((obj as any)["registered_model"]) : undefined,
         }};

        delete (jsonObj as any)["registered_model"];
        
        return jsonObj;
    }
}
