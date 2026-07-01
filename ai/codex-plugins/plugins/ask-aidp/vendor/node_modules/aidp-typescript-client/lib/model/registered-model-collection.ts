// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing RegisteredModels.
*/
export interface RegisteredModelCollection {
    /**
    * Registered models that match the search criteria.
    */
    'registeredModels': Array<model.RegisteredModel>;
    /**
    * Token that can be used to retrieve the next page of registered models.
    */
    'nextPageToken'?: string;

}

export namespace RegisteredModelCollection {



    export function getJsonObj(obj: RegisteredModelCollection): object {
        const jsonObj = {...obj, ...{
            
                'registered_models': obj.registeredModels ?
                
                obj.registeredModels.map((item)=>{return model.RegisteredModel.getJsonObj(item)})
                
                 : undefined,
                'next_page_token': obj.nextPageToken,

        }};

        delete (jsonObj as Partial<RegisteredModelCollection>).registeredModels;delete (jsonObj as Partial<RegisteredModelCollection>).nextPageToken;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RegisteredModelCollection): object {
        const jsonObj = {...obj, ...{
            
                    'registeredModels': (obj as any)["registered_models"] ?
                
                (obj as any)["registered_models"].map((item: any)=>{return model.RegisteredModel.getDeserializedJsonObj(item)})
                
                 : undefined,
                'nextPageToken': (obj as any)["next_page_token"],

         }};

        delete (jsonObj as any)["registered_models"];delete (jsonObj as any)["next_page_token"];
        
        return jsonObj;
    }
}
