// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Different types of filters
*/
export interface FilterTypes {
    /**
    * Provide the list of term filters
    */
    'listFilters'?: Array<model.ListFilter>;
    /**
    * Provide the list of range filters
    */
    'rangeFilters'?: Array<model.RangeFilter>;

}

export namespace FilterTypes {



    export function getJsonObj(obj: FilterTypes): object {
        const jsonObj = {...obj, ...{
            
                'listFilters': obj.listFilters ?
                
                obj.listFilters.map((item)=>{return model.ListFilter.getJsonObj(item)})
                
                 : undefined,
                'rangeFilters': obj.rangeFilters ?
                
                obj.rangeFilters.map((item)=>{return model.RangeFilter.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FilterTypes): object {
        const jsonObj = {...obj, ...{
            
                    'listFilters': obj.listFilters ?
                
                obj.listFilters.map((item)=>{return model.ListFilter.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'rangeFilters': obj.rangeFilters ?
                
                obj.rangeFilters.map((item)=>{return model.RangeFilter.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
