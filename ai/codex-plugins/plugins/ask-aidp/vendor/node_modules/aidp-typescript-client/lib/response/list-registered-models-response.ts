// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

export interface ListRegisteredModelsResponse {
    /**
     * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* 
     */
    'opcRequestId': string;
    /**
     * For list pagination. When this header appears in the response, additional pages of results remain. For
* important details about how pagination works, see [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
* 
     */
    'opcNextPage': string;
    /**
     * The returned model.RegisteredModelCollection instance.
     */
    'registeredModelCollection': model.RegisteredModelCollection;

}
