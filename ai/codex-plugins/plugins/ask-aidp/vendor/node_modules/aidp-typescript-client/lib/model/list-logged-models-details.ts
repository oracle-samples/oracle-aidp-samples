// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of logged models to fetch.
*/
export interface ListLoggedModelsDetails {
    /**
    * Fetch logged-models under list of experiments.
* 
    */
    'experimentIds'?: Array<string>;
    /**
    * Maximum number of logged-models desired. Servers may select a default. Callers are encouraged to 
* pass max_results explicitly and leverage page_token to iterate.
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxResults'?: number;
    /**
    * Token indicating the page of logged-models to fetch.
    */
    'pageToken'?: string;
    /**
    * A filter expression over logged-models attributes.
* 
    */
    'filter'?: string;
    /**
    * List of attributes for ordering search results.
* 
    */
    'orderBy'?: Array<model.LoggedModelOrder>;

}

export namespace ListLoggedModelsDetails {






    export function getJsonObj(obj: ListLoggedModelsDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_ids': obj.experimentIds,

                'max_results': obj.maxResults,

                'page_token': obj.pageToken,


                'order_by': obj.orderBy ?
                
                obj.orderBy.map((item)=>{return model.LoggedModelOrder.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<ListLoggedModelsDetails>).experimentIds;delete (jsonObj as Partial<ListLoggedModelsDetails>).maxResults;delete (jsonObj as Partial<ListLoggedModelsDetails>).pageToken;delete (jsonObj as Partial<ListLoggedModelsDetails>).orderBy;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ListLoggedModelsDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentIds': (obj as any)["experiment_ids"],

                'maxResults': (obj as any)["max_results"],

                'pageToken': (obj as any)["page_token"],


                    'orderBy': (obj as any)["order_by"] ?
                
                (obj as any)["order_by"].map((item: any)=>{return model.LoggedModelOrder.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["experiment_ids"];delete (jsonObj as any)["max_results"];delete (jsonObj as any)["page_token"];delete (jsonObj as any)["order_by"];
        
        return jsonObj;
    }
}
