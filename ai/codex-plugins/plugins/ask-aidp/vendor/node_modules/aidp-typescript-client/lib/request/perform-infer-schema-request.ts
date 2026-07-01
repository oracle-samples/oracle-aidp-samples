// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/PerformInferSchema.ts.html |here} to see how to use PerformInferSchemaRequest.
 */
export interface PerformInferSchemaRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The fully qualified name of the schema in the format <catalog_name>.<schema_name>.
 */
 'schemaKey': string;
/**
 * Details of the location from which the table schema can be inferred.
 */
 'performInferSchemaDetails':  model.PerformInferSchemaDetails;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
/**
 * A flag to identify if the recent list should be updated.
 */
 'shouldUpdateRecent'?: boolean;
}

