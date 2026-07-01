// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/ListModelVersions.ts.html |here} to see how to use ListModelVersionsRequest.
 */
export interface ListModelVersionsRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * String filter condition, like \"name LIKE 'my-model-name'\". Single boolean condition, with string 
* values wrapped in single quotes.
* 
 */
 'filter'?: string;
/**
 * Maximum number of model versions to retrieve.
 */
 'maxResults'?: number;
/**
 * Pagination token to go to the next page based on a previous search query.
 */
 'pageToken'?: string;
/**
 * List of columns to be ordered by including model name, version, stage with an optional \"DESC\" or \"ASC\" 
* annotation, where \"ASC\" is the default. Tiebreaks are done by latest stage transition timestamp, 
* followed by name ASC, followed by version DESC.
* 
 */
 'orderBy'?: string;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
/**
 * The DH User Principal Header .
 */
 'dhUserPrincipal'?: string;
}

