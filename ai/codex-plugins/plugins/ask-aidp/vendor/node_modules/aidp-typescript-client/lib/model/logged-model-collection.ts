// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing logged-models.
*/
export interface LoggedModelCollection {
    /**
    * Logged models that match the search criteria
    */
    'models'?: Array<model.LoggedModel>;
    /**
    * Token that can be used to retrieve the next page of logged-models. An empty token means that no more logged-models are available for retrieval.
    */
    'nextPageToken'?: string;

}

export namespace LoggedModelCollection {



    export function getJsonObj(obj: LoggedModelCollection): object {
        const jsonObj = {...obj, ...{
            
                'models': obj.models ?
                
                obj.models.map((item)=>{return model.LoggedModel.getJsonObj(item)})
                
                 : undefined,
                'next_page_token': obj.nextPageToken,

        }};

        delete (jsonObj as Partial<LoggedModelCollection>).nextPageToken;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LoggedModelCollection): object {
        const jsonObj = {...obj, ...{
            
                    'models': obj.models ?
                
                obj.models.map((item)=>{return model.LoggedModel.getDeserializedJsonObj(item)})
                
                 : undefined,
                'nextPageToken': (obj as any)["next_page_token"],

         }};

        delete (jsonObj as any)["next_page_token"];
        
        return jsonObj;
    }
}
