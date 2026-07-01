// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Model version details.
*/
export interface GetModelVersionResponseDetails {
    'modelVersion': model.ModelVersion;

}

export namespace GetModelVersionResponseDetails {


    export function getJsonObj(obj: GetModelVersionResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'model_version': obj.modelVersion ?
                
                
                model.ModelVersion.getJsonObj(obj.modelVersion) : undefined,
        }};

        delete (jsonObj as Partial<GetModelVersionResponseDetails>).modelVersion;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GetModelVersionResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'modelVersion': (obj as any)["model_version"] ?
                
                
                model.ModelVersion.getDeserializedJsonObj((obj as any)["model_version"]) : undefined,
         }};

        delete (jsonObj as any)["model_version"];
        
        return jsonObj;
    }
}
