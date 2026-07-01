// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An async operation.
*/
export interface AsyncOperation {
    /**
    * The unique key that identifies an async operation
    */
    'key': string;
    /**
    * The resource type of the async operation.
    */
    'resourceType': model.AsyncOperationResourceType;
    /**
    * The action type of the async operation.
    */
    'actionType': model.AsyncOperationActionType;
    /**
    * The fully qualified name of the Data Lake resource. Example: For table, it is 
* <catalog_name>.<schema_name>.<table_name>. For Cluster, it is <workspace_key>.<cluster_key>
* 
    */
    'resourceName': string;
    /**
    * The display name of the Data Lake resource. Example: For catalog/table/schema, it is same as resourceName
* But for workspace/cluster it is workspace and cluster displayName field.
* 
    */
    'resourceDisplayName'?: string;
    /**
    * The principal Id who started the async operation
* 
    */
    'createdBy'?: string;
    /**
    * The principal name who started the async operation
* 
    */
    'createdByName'?: string;
    /**
    * The date and time the Async operation was started, in the format defined by [RFC 3339](https://tools.ietf.org/html/rfc3339).
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeStarted': Date;
    /**
    * The date and time the Async operation finished, in the format defined by [RFC 3339](https://tools.ietf.org/html/rfc3339).
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeFinished'?: Date;
    /**
    * The state of the Table.
    */
    'status': model.AsyncOperationStatus;
    /**
    * Represents the error code of a failure
* 
    */
    'errorCode'?: string;
    /**
    * Representss extra error information of a failure
* 
    */
    'errorMessage'?: string;

}

export namespace AsyncOperation {













    export function getJsonObj(obj: AsyncOperation): object {
        const jsonObj = {...obj, ...{
            












        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AsyncOperation): object {
        const jsonObj = {...obj, ...{
            












         }};

        
        
        return jsonObj;
    }
}
