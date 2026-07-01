// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/ListArtifacts.ts.html |here} to see how to use ListArtifactsRequest.
 */
export interface ListArtifactsRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the Workspace
 */
 'workspaceKey': string;
/**
 * ID of the run whose artifacts to list.
 */
 'runId': string;
/**
 * Filter artifacts matching this path (a relative path from the root artifact directory).
 */
 'path'?: string;
/**
 * Token indicating the page of artifact results to fetch.
 */
 'pageToken'?: string;
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

