// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response payload containing search results and metadata.
*/
export interface ClusterLogsSummary {
    /**
    * List of log field schema information.
    */
    'fields'?: Array<model.FieldInfo>;
    /**
    * List of search results.
    */
    'results': Array<model.SearchResult>;
    'summary': model.SearchResultSummary;

}

export namespace ClusterLogsSummary {




    export function getJsonObj(obj: ClusterLogsSummary): object {
        const jsonObj = {...obj, ...{
            
                'fields': obj.fields ?
                
                obj.fields.map((item)=>{return model.FieldInfo.getJsonObj(item)})
                
                 : undefined,
                'results': obj.results ?
                
                obj.results.map((item)=>{return model.SearchResult.getJsonObj(item)})
                
                 : undefined,
                'summary': obj.summary ?
                
                
                model.SearchResultSummary.getJsonObj(obj.summary) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterLogsSummary): object {
        const jsonObj = {...obj, ...{
            
                    'fields': obj.fields ?
                
                obj.fields.map((item)=>{return model.FieldInfo.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'results': obj.results ?
                
                obj.results.map((item)=>{return model.SearchResult.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'summary': obj.summary ?
                
                
                model.SearchResultSummary.getDeserializedJsonObj(obj.summary) : undefined,
         }};

        
        
        return jsonObj;
    }
}
