// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of experiments to fetch.
*/
export interface ListExperimentsDetails {
    /**
    * Maximum number of experiments desired. Servers may select a default. All servers are guaranteed to 
* support a max_results threshold of at least 1,000 but may support more. Callers are encouraged to 
* pass max_results explicitly and leverage page_token to iterate.
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxResults'?: number;
    /**
    * Token indicating the page of experiments to fetch.
    */
    'pageToken'?: string;
    /**
    * A filter expression over experiment attributes and tags that allows returning a subset of experiments.
* The syntax is a subset of SQL that supports ANDing together binary operations between an attribute or tag and a constant.
* Example: name LIKE 'test-%' AND tags.key = 'value'
* Columns with special characters (hyphen, space, period, etc.) can be selected using double quotes or backticks.
* Example: tags.\"extra-key\" = 'value' or tags.{@code extra-key} = 'value'
* Supported operators are =, !=, LIKE, and ILIKE.
* 
    */
    'filter'?: string;
    /**
    * List of columns for ordering search results, which can include experiment name and ID with 
* an optional \"DESC\" or \"ASC\" annotation, where \"ASC\" is the default. Tiebreaks are done by experiment ID DESC.
* 
    */
    'orderBy'?: Array<string>;
    /**
    * Qualifier for type of experiments to be returned. If unspecified, returns only active experiments.
    */
    'viewType'?: ListExperimentsDetails.ViewType;

}

export namespace ListExperimentsDetails {





    export enum ViewType {
    
    ActiveOnly = "ACTIVE_ONLY",
    DeletedOnly = "DELETED_ONLY",
    All = "ALL"

}


    export function getJsonObj(obj: ListExperimentsDetails): object {
        const jsonObj = {...obj, ...{
            
                'max_results': obj.maxResults,

                'page_token': obj.pageToken,


                'order_by': obj.orderBy,

                'view_type': obj.viewType,

        }};

        delete (jsonObj as Partial<ListExperimentsDetails>).maxResults;delete (jsonObj as Partial<ListExperimentsDetails>).pageToken;delete (jsonObj as Partial<ListExperimentsDetails>).orderBy;delete (jsonObj as Partial<ListExperimentsDetails>).viewType;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ListExperimentsDetails): object {
        const jsonObj = {...obj, ...{
            
                'maxResults': (obj as any)["max_results"],

                'pageToken': (obj as any)["page_token"],


                'orderBy': (obj as any)["order_by"],

                'viewType': (obj as any)["view_type"],

         }};

        delete (jsonObj as any)["max_results"];delete (jsonObj as any)["page_token"];delete (jsonObj as any)["order_by"];delete (jsonObj as any)["view_type"];
        
        return jsonObj;
    }
}
