// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

export interface GenerateTempFileUploadTargetResponse {
    /**
     * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* 
     */
    'opcRequestId': string;
    /**
     * The returned model.GenerateTempFileUploadTargetResponseDetails instance.
     */
    'generateTempFileUploadTargetResponseDetails': model.GenerateTempFileUploadTargetResponseDetails;

}
