/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
export interface DelayStrategy {
    delay(context: WaitContext): number;
}
export interface TerminationStrategy {
    shouldTerminate(context: WaitContext): boolean;
}
export interface WaitContext {
    readonly startTime: Date;
    readonly attemptCount: number;
}
export declare class WaitContextImpl implements WaitContext {
    readonly startTime: Date;
    attemptCount: number;
}
export declare class ExponentialBackoffDelayStrategy implements DelayStrategy {
    protected maxDelayInSeconds: number;
    constructor(maxDelayInSeconds: number);
    delay(context: WaitContext): number;
}
export declare class ExponentialBackoffDelayStrategyWithJitter extends ExponentialBackoffDelayStrategy {
    protected maxDelayInSeconds: number;
    constructor(maxDelayInSeconds: number);
    delay(context: WaitContext): number;
}
export declare class FixedTimeDelayStrategy implements DelayStrategy {
    private timeBetweenAttempsInSeconds;
    constructor(timeBetweenAttempsInSeconds: number);
    delay(context: WaitContext): number;
}
export declare class MaxAttemptsTerminationStrategy implements TerminationStrategy {
    private _maxAttempts;
    constructor(maxAttempts: number);
    shouldTerminate(context: WaitContext): boolean;
    get maxAttempts(): number;
}
export declare class MaxTimeTerminationStrategy implements TerminationStrategy {
    private maxTimeInSeconds;
    constructor(maxTimeInSeconds: number);
    shouldTerminate(context: WaitContext): boolean;
}
export interface WaiterConfigurationDetails {
    terminationStrategy: TerminationStrategy;
    delayStrategy: DelayStrategy;
}
export declare type WaiterConfiguration = Partial<WaiterConfigurationDetails>;
export declare function delay(second: number): Promise<unknown>;
export declare function genericTerminalConditionWaiter<Response>(config: WaiterConfiguration | undefined, serviceCall: () => Promise<Response>, terminationPredicate: (response: Response) => boolean, allow404?: boolean): Promise<Response | null>;
export declare function genericWaiter<Response>(config: WaiterConfiguration | undefined, serviceCall: () => Promise<Response>, terminationPredicate: (response: Response) => boolean): Promise<Response>;
