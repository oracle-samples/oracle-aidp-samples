// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

export interface DownloadClusterLogsResponse {
    /**
     * The key of the asynchronous operations associated with an AI Data Platform instance.
* Use GetAsyncOperation with this key to track the status of the request.
* 
     */
    'aidpAsyncOperationKey': string;
    /**
     * This string represents the PAR URL for the compute log file. The {@code datalake-cluster-log-par-url} should be used only after the
* {@code aidp-async-operation-key} status reaches the SUCCEEDED state. If accessed before the operation completes, the file may be incomplete.
* 
     */
    'datalakeClusterLogParUrl': string;
    /**
     * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* 
     */
    'opcRequestId': string;


}
