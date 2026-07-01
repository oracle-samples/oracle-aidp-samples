// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

export interface CreateDataTableResponse {
    /**
     * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the asynchronous work request.
* Use GetWorkRequest with this ID to track the status of the request.
* 
     */
    'opcWorkRequestId': string;
    /**
     * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* 
     */
    'opcRequestId': string;
    /**
     * The key of the asynchronous operations associated with an AI Data Platform instance.
* Use GetAsyncOperation with this key to track the status of the request.
* 
     */
    'aidpAsyncOperationKey': string;


}
