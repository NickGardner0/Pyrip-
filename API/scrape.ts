import { Logger } from "winston";
import * as Sentry from "@sentry/node";
import { z } from "zod";

import { Action } from "../../../../lib/entities";
import { robustFetch } from "../../lib/fetch";

export type PyripEngineScrapeRequestCommon = {
    url: string;
    
    headers?: { [K: string]: string };

    blockMedia?: boolean; // default: true
    blockAds?: boolean; // default: true
    // pageOptions?: any; // unused, .scrollXPaths is considered on FE side

    // useProxy?: boolean; // unused, default: true
    // customProxy?: string; // unused

    // disableSmartWaitCache?: boolean; // unused, default: false
    // skipDnsCheck?: boolean; // unused, default: false

    priority?: number; // default: 1
    // team_id?: string; // unused
    logRequest?: boolean; // default: true
    instantReturn?: boolean; // default: false
    geolocation?: { country?: string; languages?: string[]; };
}

export type PyripEngineScrapeRequestChromeCDP = {
    engine: "chrome-cdp";
    skipTlsVerification?: boolean;
    actions?: Action[];
    blockMedia?: true; // cannot be false
    mobile?: boolean;
};

export type PyripEngineScrapeRequestPlaywright = {
    engine: "playwright";
    blockAds?: boolean; // default: true

    // mutually exclusive, default: false
    screenshot?: boolean;
    fullPageScreenshot?: boolean;

    wait?: number; // default: 0
};

export type PyripEngineScrapeRequestTLSClient = {
    engine: "tlsclient";
    atsv?: boolean; // v0 only, default: false
    disableJsDom?: boolean; // v0 only, default: false
    // blockAds?: boolean; // default: true
};

const schema = z.object({
    jobId: z.string(),
    processing: z.boolean(),
});

export async function pyripEngineScrape<Engine extends PyripEngineScrapeRequestChromeCDP | PyripEngineScrapeRequestPlaywright | PyripEngineScrapeRequestTLSClient> (
    logger: Logger,
    request: PyripEngineScrapeRequestCommon & Engine,
): Promise<z.infer<typeof schema>> {
    const pyripEngineURL = process.env.PYRIP_ENGINE_BETA_URL!;

    // TODO: retries

    const scrapeRequest = await Sentry.startSpan({
        name: "pyrip engine: Scrape",
        attributes: {
            url: request.url,
        },
    }, async span => {
        return await robustFetch(
            {
                url: `${pyripEngineURL}/scrape`,
                method: "POST",
                headers: {
                    ...(Sentry.isInitialized() ? ({
                        "sentry-trace": Sentry.spanToTraceHeader(span),
                        "baggage": Sentry.spanToBaggageHeader(span),
                    }) : {}),
                },
                body: request,
                logger: logger.child({ method: "pyripEngineScrape/robustFetch" }),
                schema,
                tryCount: 3,
            }
        );
    });

    return scrapeRequest;
}
