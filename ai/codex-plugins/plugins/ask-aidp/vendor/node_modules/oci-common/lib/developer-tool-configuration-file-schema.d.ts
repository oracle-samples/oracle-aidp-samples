import { RegionMetadataSchema } from "./region-metadata-schema";
export interface DeveloperToolConfigurationFileSchema {
    /**
     * Class representing DeveloperToolConfigurationFileSchema blob that can be used for parsing out developer tool configuration regions, enabled services,
     * and the developer tool configuration provider name
     */
    regions: RegionMetadataSchema[];
    services: string[];
    developerToolConfigurationProvider: string;
    allowOnlyDeveloperToolConfigurationRegions: string;
}
