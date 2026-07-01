// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Created model version details.
*/
export interface CreateModelVersionResponseDetails {
    'modelVersion': model.ModelVersion;

}

export namespace CreateModelVersionResponseDetails {


    export function getJsonObj(obj: CreateModelVersionResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'model_version': obj.modelVersion ?
                
                
                model.ModelVersion.getJsonObj(obj.modelVersion) : undefined,
        }};

        delete (jsonObj as Partial<CreateModelVersionResponseDetails>).modelVersion;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateModelVersionResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'modelVersion': (obj as any)["model_version"] ?
                
                
                model.ModelVersion.getDeserializedJsonObj((obj as any)["model_version"]) : undefined,
         }};

        delete (jsonObj as any)["model_version"];
        
        return jsonObj;
    }
}
