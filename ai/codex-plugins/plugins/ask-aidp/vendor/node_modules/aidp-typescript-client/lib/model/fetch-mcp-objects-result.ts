// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response for a list tools call
*/
export interface FetchMcpObjectsResult {
    /**
    * The list of objects to be returned.
    */
    'objects'?: Array<model.McpObject>;
    /**
    * For list pagination. When this header appears in the response, additional pages of results remain. For
* important details about how pagination works, see [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
* 
    */
    'nextPage'?: string;
    /**
    * For list pagination. When this header appears in the response, previous pages of results remain. For
* important details about how pagination works, see [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
* 
    */
    'previousPage'?: string;
    /**
    * For list pagination. This header provides total number of items available. For
* important details about how pagination works, see [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'totalItems'?: number;

}

export namespace FetchMcpObjectsResult {





    export function getJsonObj(obj: FetchMcpObjectsResult): object {
        const jsonObj = {...obj, ...{
            
                'objects': obj.objects ?
                
                obj.objects.map((item)=>{return model.McpObject.getJsonObj(item)})
                
                 : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FetchMcpObjectsResult): object {
        const jsonObj = {...obj, ...{
            
                    'objects': obj.objects ?
                
                obj.objects.map((item)=>{return model.McpObject.getDeserializedJsonObj(item)})
                
                 : undefined,



         }};

        
        
        return jsonObj;
    }
}
