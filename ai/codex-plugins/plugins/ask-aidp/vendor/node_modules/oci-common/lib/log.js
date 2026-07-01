"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.LOG = void 0;
var LOG;
(function (LOG) {
    var _logger;
    Object.defineProperty(LOG, "logger", {
        get: function () {
            return _logger;
        },
        set: function (log) {
            _logger = log;
        }
    });
})(LOG = exports.LOG || (exports.LOG = {}));
//# sourceMappingURL=log.js.map