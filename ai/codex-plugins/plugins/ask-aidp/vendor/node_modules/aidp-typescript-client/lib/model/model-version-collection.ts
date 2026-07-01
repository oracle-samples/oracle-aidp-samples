// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing model versions.
*/
export interface ModelVersionCollection {
    /**
    * Model versions that match the search criteria.
    */
    'modelVersions': Array<model.ModelVersion>;
    /**
    * Token that can be used to retrieve the next page of model versions.
    */
    'nextPageToken'?: string;

}

export namespace ModelVersionCollection {



    export function getJsonObj(obj: ModelVersionCollection): object {
        const jsonObj = {...obj, ...{
            
                'model_versions': obj.modelVersions ?
                
                obj.modelVersions.map((item)=>{return model.ModelVersion.getJsonObj(item)})
                
                 : undefined,
                'next_page_token': obj.nextPageToken,

        }};

        delete (jsonObj as Partial<ModelVersionCollection>).modelVersions;delete (jsonObj as Partial<ModelVersionCollection>).nextPageToken;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelVersionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'modelVersions': (obj as any)["model_versions"] ?
                
                (obj as any)["model_versions"].map((item: any)=>{return model.ModelVersion.getDeserializedJsonObj(item)})
                
                 : undefined,
                'nextPageToken': (obj as any)["next_page_token"],

         }};

        delete (jsonObj as any)["model_versions"];delete (jsonObj as any)["next_page_token"];
        
        return jsonObj;
    }
}
