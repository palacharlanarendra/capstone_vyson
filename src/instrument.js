import * as Sentry from "@sentry/node";
import 'dotenv/config';

Sentry.init({
  dsn: process.env.SENTRY_DSN || "https://placeholder-key@o0.ingest.sentry.io/0",
  tracesSampleRate: 1.0,
});
