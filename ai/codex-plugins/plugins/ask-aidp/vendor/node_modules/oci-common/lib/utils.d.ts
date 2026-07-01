/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

 * Utility method to check if environment is node or browser
 */
import { ClientConfiguration } from "./client-configuration";
export declare function isBrowser(): boolean;
export declare function isEmpty(obj: any): boolean;
export declare function checkNotNull(value: string | null, msg: string): string;
export declare function isCircuitBreakerSystemEnabled(clientConfiguration: ClientConfiguration): boolean;
