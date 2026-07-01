/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
interface CircuitBreakerOptions {
    timeout?: number;
    resetTimeout?: number;
    rollingCountTimeout?: number;
    rollingCountBuckets?: number;
    name?: string;
    rollingPercentilesEnabled?: boolean;
    capacity?: number;
    errorThresholdPercentage?: number;
    enabled?: boolean;
    allowWarmUp?: boolean;
    volumeThreshold?: number;
    errorFilter?: Function;
    cache?: boolean;
    disableClientCircuitBreaker?: boolean;
}
export default class CircuitBreaker {
    circuit: any;
    noCircuit: boolean;
    static get envVariableCheckForDefaultCircuitBreaker(): string | undefined;
    static EnableGlobalCircuitBreaker: boolean;
    static EnableDefaultCircuitBreaker: string | undefined;
    static DefaultCircuitBreakerOverriden: boolean;
    private static DefaultConfiguration;
    private static MIN_WAIT_DURATION_IN_OPEN_STATE_FOR_AUTH_CLIENT_CB;
    private static MAX_WAIT_DURATION_IN_OPEN_STATE_FOR_AUTH_CLIENT_CB;
    static DefaultAuthConfiguration: CircuitBreakerOptions;
    static get defaultAuthConfiguration(): CircuitBreakerOptions;
    static get defaultConfiguration(): CircuitBreakerOptions;
    static set defaultConfiguration(circuitBreakerConfig: CircuitBreakerOptions);
    constructor(options?: CircuitBreakerOptions);
}
export {};
