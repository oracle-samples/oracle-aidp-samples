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
const abstract_requesting_authentication_detail_provider_1 = __importDefault(require("./abstract-requesting-authentication-detail-provider"));
const abstract_federation_client_authenticated_details_provider_builder_1 = __importDefault(require("./abstract-federation-client-authenticated-details-provider-builder"));
class InstancePrincipalsAuthenticationDetailsProvider extends abstract_requesting_authentication_detail_provider_1.default {
    constructor(federationClient, sessionKeySupplier, region) {
        super(federationClient, sessionKeySupplier);
        this.federationClient = federationClient;
        this.sessionKeySupplier = sessionKeySupplier;
        this.region = region;
    }
    /**
     * Refreshes the authentication data used by the provider
     * @return the refreshed authentication data
     */
    refresh() {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this.federationClient.refreshAndGetSecurityToken();
        });
    }
    // region getter
    getRegion() {
        return this.region;
    }
    /**
     * Creates a new InstancePrincipalsAuthenticationDetailsProviderBuilder.
     * @return A new builder instance.
     */
    static builder() {
        return new InstancePrincipalsAuthenticationDetailsProviderBuilder();
    }
}
class InstancePrincipalsAuthenticationDetailsProviderBuilder extends abstract_federation_client_authenticated_details_provider_builder_1.default {
    constructor(purpose) {
        super();
        if (purpose) {
            this.purpose = purpose;
        }
    }
    build() {
        const _super = Object.create(null, {
            build: { get: () => super.build }
        });
        return __awaiter(this, void 0, void 0, function* () {
            yield this.autoDetectUsingMetadataUrl();
            return yield _super.build.call(this);
        });
    }
    buildProvider(sessionKeySupplierToUse) {
        return new InstancePrincipalsAuthenticationDetailsProvider(this._federationClient, sessionKeySupplierToUse, this.region);
    }
    set federationEndpoint(federationEndpoint) {
        super._federationEndpoint = federationEndpoint;
    }
    set leafCertificateSupplier(leafCertificateSupplier) {
        super._leafCertificateSupplier = leafCertificateSupplier;
    }
}
exports.default = InstancePrincipalsAuthenticationDetailsProviderBuilder;
//# sourceMappingURL=instance-principals-authentication-detail-provider.js.map