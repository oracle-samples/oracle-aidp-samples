// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of experiment runs to fetch.
*/
export interface ListExperimentRunsDetails {
    /**
    * List of experiment IDs to search over.
    */
    'experimentIds'?: Array<string>;
    /**
    * Maximum number of runs desired. If unspecified, defaults to 1000. All servers are guaranteed to 
* support a max_results threshold of at least 50,000 but may support more. Callers are encouraged to 
* pass max_results explicitly and leverage page_token to iterate.
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxResults'?: number;
    /**
    * Token indicating the page of experiment runs to fetch.
    */
    'pageToken'?: string;
    /**
    * A filter expression over params, metrics, and tags, that allows returning a subset of runs. The syntax is 
* a subset of SQL that supports ANDing together binary operations between a param, metric, or tag and a constant.
* Example: metrics.rmse < 1 and params.model_class = 'LogisticRegression'
* You can select columns with special characters (hyphen, space, period, etc.) by using 
* double quotes: metrics.\"model class\" = 'LinearRegression' and tags.\"user-name\" = 'Tomas'
* Supported operators are =, !=, >, >=, <, and <=.
* 
    */
    'filter'?: string;
    /**
    * List of columns to be ordered by, including attributes, params, metrics, and tags with an 
* optional \"DESC\" or \"ASC\" annotation, where \"ASC\" is the default. 
* Example: [\"params.input DESC\", \"metrics.alpha ASC\", \"metrics.rmse\"] 
* Tiebreaks are done by start_time DESC followed by run_id for runs with the same start time (and this is 
* the default ordering criterion if order_by is not provided).
* 
    */
    'orderBy'?: Array<string>;
    /**
    * Qualifier for type of runs to be returned. If unspecified, returns only active runs.
    */
    'runViewType'?: ListExperimentRunsDetails.RunViewType;

}

export namespace ListExperimentRunsDetails {






    export enum RunViewType {
    
    ActiveOnly = "ACTIVE_ONLY",
    DeletedOnly = "DELETED_ONLY",
    All = "ALL"

}


    export function getJsonObj(obj: ListExperimentRunsDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_ids': obj.experimentIds,

                'max_results': obj.maxResults,

                'page_token': obj.pageToken,


                'order_by': obj.orderBy,

                'run_view_type': obj.runViewType,

        }};

        delete (jsonObj as Partial<ListExperimentRunsDetails>).experimentIds;delete (jsonObj as Partial<ListExperimentRunsDetails>).maxResults;delete (jsonObj as Partial<ListExperimentRunsDetails>).pageToken;delete (jsonObj as Partial<ListExperimentRunsDetails>).orderBy;delete (jsonObj as Partial<ListExperimentRunsDetails>).runViewType;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ListExperimentRunsDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentIds': (obj as any)["experiment_ids"],

                'maxResults': (obj as any)["max_results"],

                'pageToken': (obj as any)["page_token"],


                'orderBy': (obj as any)["order_by"],

                'runViewType': (obj as any)["run_view_type"],

         }};

        delete (jsonObj as any)["experiment_ids"];delete (jsonObj as any)["max_results"];delete (jsonObj as any)["page_token"];delete (jsonObj as any)["order_by"];delete (jsonObj as any)["run_view_type"];
        
        return jsonObj;
    }
}
