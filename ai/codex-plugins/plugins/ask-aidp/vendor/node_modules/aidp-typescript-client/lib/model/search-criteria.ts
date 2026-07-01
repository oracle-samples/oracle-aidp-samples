// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details about search criteria
*/
export interface SearchCriteria {
    /**
    * Search query string
    */
    'query'?: string;
    /**
    * The maximum number of items to return. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'limit'?: number;
    /**
    * The doc number from which it needs to be return. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'offset'?: number;
    /**
    * The provide the field name on which we need aggregation.
    */
    'aggregations'?: Array<model.AggregationEnum>;
    'filter'?: model.FilterTypes;
    /**
    * Array of objects having details about sort field and order.
    */
    'sort'?: Array<model.SortDetails>;

}

export namespace SearchCriteria {







    export function getJsonObj(obj: SearchCriteria): object {
        const jsonObj = {...obj, ...{
            



                'aggregations': obj.aggregations ?
                
                obj.aggregations.map((item)=>{return model.AggregationEnum.getJsonObj(item)})
                
                 : undefined,
                'filter': obj.filter ?
                
                
                model.FilterTypes.getJsonObj(obj.filter) : undefined,
                'sort': obj.sort ?
                
                obj.sort.map((item)=>{return model.SortDetails.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SearchCriteria): object {
        const jsonObj = {...obj, ...{
            



                    'aggregations': obj.aggregations ?
                
                obj.aggregations.map((item)=>{return model.AggregationEnum.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'filter': obj.filter ?
                
                
                model.FilterTypes.getDeserializedJsonObj(obj.filter) : undefined,
                    'sort': obj.sort ?
                
                obj.sort.map((item)=>{return model.SortDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
