/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

 * This header is excluded from headers.ts as we need to use same retry token but new request token
 * which needs to be incorporated when we add retry strategy.
 */
export declare const OPC_RETRY_TOKEN_HEADER: string;
export declare function addRetryToken(headers: Headers): void;
