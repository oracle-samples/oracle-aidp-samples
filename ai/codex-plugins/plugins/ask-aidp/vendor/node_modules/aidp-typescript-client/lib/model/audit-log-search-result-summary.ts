// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Search result for audit log search request.
*/
export interface AuditLogSearchResultSummary {
    /**
    * Unique ID of the result.
    */
    'eventId'?: string;
    /**
    * Time of the log.
    */
    'timeOfLog'?: Date;
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
    * Source of logs.
    */
    'source'?: string;
    /**
    * Payload of logs.
    */
    'payload'?: string;

}

export namespace AuditLogSearchResultSummary {










    export function getJsonObj(obj: AuditLogSearchResultSummary): object {
        const jsonObj = {...obj, ...{
            









        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AuditLogSearchResultSummary): object {
        const jsonObj = {...obj, ...{
            









         }};

        
        
        return jsonObj;
    }
}
