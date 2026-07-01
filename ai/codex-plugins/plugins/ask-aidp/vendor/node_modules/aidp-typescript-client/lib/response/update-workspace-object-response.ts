// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");
import stream = require("stream");

export interface UpdateWorkspaceObjectResponse {
    /**
     * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* 
     */
    'opcRequestId': string;
    /**
     * For optimistic concurrency control. See {@code if-match}.
* 
     */
    'etag': string;
    /**
     * Unique key of the object.
* 
     */
    'objectKey': string;
    /**
     * The full path of the object.
* 
     */
    'path': string;
    /**
     * Type of the object
* 
     */
    'type': string;
    /**
     * The date and time when Workspace Object was updated, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
     */
    'timeUpdated': Date;
    /**
     * The returned stream.Readable | ReadableStream instance.
     */
    'value': stream.Readable | ReadableStream;

}
