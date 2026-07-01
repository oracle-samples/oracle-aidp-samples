// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information needed to search the audit logs.
*/
export interface SearchAuditLogsDetails {
    /**
    * Start time of the logs.
    */
    'timeBegin'?: Date;
    /**
    * End time of the logs.
    */
    'timeEnd'?: Date;
    /**
    * Type of object.
    */
    'objectType'?: model.ObjectType;
    /**
    * Name of the object.
    */
    'objectName'?: string;
    /**
    * Type of operation.
    */
    'operation'?: model.Operation;
    /**
    * Status of log.
    */
    'status'?: model.Status;
    /**
    * Operation started by.
    */
    'startedBy'?: string;
    /**
    * Query to search the log.
    */
    'query'?: string;
    /**
    * The field to sort by.
* 
    */
    'sortBy'?: SearchAuditLogsDetails.SortBy;
    /**
    * Sort order for search results.
    */
    'sortOrder'?: SearchAuditLogsDetails.SortOrder;

}

export namespace SearchAuditLogsDetails {









    export enum SortBy {
    
    StartedBy = "startedBy",
    Status = "status",
    Operation = "operation",
    ObjectType = "objectType",
    ObjectName = "objectName",
    Time = "time"

}


    export enum SortOrder {
    
    Asc = "ASC",
    Desc = "DESC"

}


    export function getJsonObj(obj: SearchAuditLogsDetails): object {
        const jsonObj = {...obj, ...{
            










        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SearchAuditLogsDetails): object {
        const jsonObj = {...obj, ...{
            










         }};

        
        
        return jsonObj;
    }
}
