"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.genericWaiter = exports.genericTerminalConditionWaiter = exports.delay = exports.MaxTimeTerminationStrategy = exports.MaxAttemptsTerminationStrategy = exports.FixedTimeDelayStrategy = exports.ExponentialBackoffDelayStrategyWithJitter = exports.ExponentialBackoffDelayStrategy = exports.WaitContextImpl = void 0;
const __1 = require("..");
class WaitContextImpl {
    constructor() {
        this.startTime = new Date();
        this.attemptCount = 0;
    }
}
exports.WaitContextImpl = WaitContextImpl;
class ExponentialBackoffDelayStrategy {
    constructor(maxDelayInSeconds) {
        this.maxDelayInSeconds = maxDelayInSeconds;
    }
    delay(context) {
        const currentDelayInSeconds = Math.pow(2, context.attemptCount);
        const delay = Math.min(currentDelayInSeconds, this.maxDelayInSeconds);
        return delay;
    }
}
exports.ExponentialBackoffDelayStrategy = ExponentialBackoffDelayStrategy;
class ExponentialBackoffDelayStrategyWithJitter extends ExponentialBackoffDelayStrategy {
    constructor(maxDelayInSeconds) {
        super(maxDelayInSeconds);
        this.maxDelayInSeconds = maxDelayInSeconds;
    }
    delay(context) {
        let jitterValue = Math.round(Math.random() * 1000) / 1000;
        return super.delay(context) + jitterValue;
    }
}
exports.ExponentialBackoffDelayStrategyWithJitter = ExponentialBackoffDelayStrategyWithJitter;
class FixedTimeDelayStrategy {
    constructor(timeBetweenAttempsInSeconds) {
        this.timeBetweenAttempsInSeconds = timeBetweenAttempsInSeconds;
    }
    delay(context) {
        return this.timeBetweenAttempsInSeconds;
    }
}
exports.FixedTimeDelayStrategy = FixedTimeDelayStrategy;
class MaxAttemptsTerminationStrategy {
    constructor(maxAttempts) {
        this._maxAttempts = maxAttempts - 1;
    }
    shouldTerminate(context) {
        return context.attemptCount >= this._maxAttempts;
    }
    get maxAttempts() {
        return this._maxAttempts;
    }
}
exports.MaxAttemptsTerminationStrategy = MaxAttemptsTerminationStrategy;
class MaxTimeTerminationStrategy {
    constructor(maxTimeInSeconds) {
        this.maxTimeInSeconds = maxTimeInSeconds;
    }
    shouldTerminate(context) {
        const endTime = new Date();
        endTime.setTime(context.startTime.getTime() + this.maxTimeInSeconds * 1000);
        return new Date() >= endTime;
    }
}
exports.MaxTimeTerminationStrategy = MaxTimeTerminationStrategy;
const DefaultWaiterConfigurationDetails = {
    terminationStrategy: new MaxTimeTerminationStrategy(1200),
    delayStrategy: new ExponentialBackoffDelayStrategy(30)
};
function delay(second) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise(resolve => setTimeout(resolve, second * 1000));
    });
}
exports.delay = delay;
function genericTerminalConditionWaiter(config, serviceCall, terminationPredicate, allow404 = false) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            return genericWaiter(config, serviceCall, terminationPredicate);
        }
        catch (ex) {
            if (ex instanceof __1.OciError && ex.statusCode == 404 && allow404) {
                return null;
            }
            throw ex;
        }
    });
}
exports.genericTerminalConditionWaiter = genericTerminalConditionWaiter;
function genericWaiter(config, serviceCall, terminationPredicate) {
    return __awaiter(this, void 0, void 0, function* () {
        const waitContext = new WaitContextImpl();
        const { terminationStrategy, delayStrategy } = Object.assign(Object.assign({}, DefaultWaiterConfigurationDetails), config);
        while (true) {
            const response = yield serviceCall();
            if (terminationPredicate(response)) {
                return response;
            }
            if (terminationStrategy.shouldTerminate(waitContext)) {
                throw Error(`Termination strategy decided to terminate with context at: ${waitContext}`);
            }
            yield delay(delayStrategy.delay(waitContext));
            waitContext.attemptCount++;
        }
    });
}
exports.genericWaiter = genericWaiter;
//# sourceMappingURL=waiter.js.map