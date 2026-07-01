// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Search results for audit log search request.
*/
export interface AuditLogSearchResultCollection {
    /**
    * Audit log search results.
    */
    'items': Array<model.AuditLogSearchResultSummary>;

}

export namespace AuditLogSearchResultCollection {


    export function getJsonObj(obj: AuditLogSearchResultCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.AuditLogSearchResultSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AuditLogSearchResultCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.AuditLogSearchResultSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
