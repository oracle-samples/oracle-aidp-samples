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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.delegateAuthProvider = void 0;
const instance_principals_authentication_detail_provider_1 = __importDefault(require("../instance-principals-authentication-detail-provider"));
function delegateAuthProvider(authenticationDetailsProvider) {
    return __awaiter(this, void 0, void 0, function* () {
        // Always make the check to see if there is a true authenticationDetailsProvider to use
        let provider = null;
        if (authenticationDetailsProvider.getProvider && authenticationDetailsProvider.getProvider()) {
            provider = authenticationDetailsProvider.getProvider();
        }
        else if (authenticationDetailsProvider.getAuthType &&
            authenticationDetailsProvider.getAuthType()) {
            // Check if authenticationDetialsProvider contain a auth_type,
            // if so, we want to set and use the appropriate auth provider.
            const authType = authenticationDetailsProvider.getAuthType();
            if (authType === "instance_principal") {
                provider = yield new instance_principals_authentication_detail_provider_1.default().build();
                authenticationDetailsProvider.setProvider(provider);
            }
        }
        return provider;
    });
}
exports.delegateAuthProvider = delegateAuthProvider;
//# sourceMappingURL=delegate-auth-provider.js.map