// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

export interface CreateVolumeResponse {
    /**
     * URL for the created volume. The volume key is generated after this request is sent.
     */
    'location': string;
    /**
     * Same as location.
     */
    'contentLocation': string;
    /**
     * For optimistic concurrency control. See {@code if-match}.
* 
     */
    'etag': string;
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
     * The returned model.Volume instance.
     */
    'volume': model.Volume;

}
