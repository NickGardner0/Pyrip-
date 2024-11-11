import { Logger } from "winston";
import { Meta } from "../..";
import { pyripEngineScrape, PyripEngineScrapeRequestChromeCDP, PyripEngineScrapeRequestCommon, PyripEngineScrapeRequestPlaywright, PyripEngineScrapeRequestTLSClient } from "./scrape";
import { EngineScrapeResult } from "..";
import { pyripEngineCheckStatus, PyripEngineCheckStatusSuccess, StillProcessingError } from "./checkStatus";
import { EngineError, TimeoutError } from "../../error";
import * as Sentry from "@sentry/node";
import { Action } from "../../../../lib/entities";
import { specialtyScrapeCheck } from "../utils/specialtyHandler";

export const defaultTimeout = 10000;

// This function does not take `Meta` on purpose. It may not access any
// meta values to construct the request -- that must be done by the
// `scrapeURLWithPyripEngine*` functions.
async function performPyripEngineScrape<Engine extends PyripEngineScrapeRequestChromeCDP | PyripEngineScrapeRequestPlaywright | PyripEngineScrapeRequestTLSClient>(
    logger: Logger,
    request: PyripEngineScrapeRequestCommon & Engine,
    timeout = defaultTimeout,
): Promise<PyripEngineCheckStatusSuccess> {
    const scrape = await pyripEngineScrape(logger.child({ method: "pyripEngineScrape" }), request);

    const startTime = Date.now();
    const errorLimit = 3;
    let errors: any[] = [];
    let status: PyripEngineCheckStatusSuccess | undefined = undefined;

    while (status === undefined) {
        if (errors.length >= errorLimit) {
            logger.error("Error limit hit.", { errors });
            throw new Error("Error limit hit. See e.cause.errors for errors.", { cause: { errors } });
        }

        if (Date.now() - startTime > timeout) {
            logger.info("Pyrip engine was unable to scrape the page before timing out.", { errors, timeout });
            throw new TimeoutError("Pyrip engine was unable to scrape the page before timing out", { cause: { errors, timeout } });
        }

        try {
            status = await pyripEngineCheckStatus(logger.child({ method: "pyripEngineCheckStatus" }), scrape.jobId)
        } catch (error) {
            if (error instanceof StillProcessingError) {
                logger.debug("Scrape is still processing...");
            } else if (error instanceof EngineError) {
                logger.debug("Pyrip engine scrape job failed.", { error, jobId: scrape.jobId });
                throw error;
            } else {
                Sentry.captureException(error);
                errors.push(error);
                logger.debug(`An unexpected error occurred while calling checkStatus. Error counter is now at ${errors.length}.`, { error, jobId: scrape.jobId });
            }
        }

        await new Promise((resolve) => setTimeout(resolve, 250));
    }

    return status;
}

export async function scrapeURLWithPyripEngineChromeCDP(meta: Meta): Promise<EngineScrapeResult> {
    const actions: Action[] = [
        // Transform waitFor option into an action (unsupported by chrome-cdp)
        ...(meta.options.waitFor !== 0 ? [{
            type: "wait" as const,
            milliseconds: meta.options.waitFor,
        }] : []),

        // Transform screenshot format into an action (unsupported by chrome-cdp)
        ...(meta.options.formats.includes("screenshot") || meta.options.formats.includes("screenshot@fullPage") ? [{
            type: "screenshot" as const,
            fullPage: meta.options.formats.includes("screenshot@fullPage"),
        }] : []),

        // Include specified actions
        ...(meta.options.actions ?? []),
    ];

    const request: PyripEngineScrapeRequestCommon & PyripEngineScrapeRequestChromeCDP = {
        url: meta.url,
        engine: "chrome-cdp",
        instantReturn: true,
        skipTlsVerification: meta.options.skipTlsVerification,
        headers: meta.options.headers,
        ...(actions.length > 0 ? ({
            actions,
        }) : {}),
        priority
