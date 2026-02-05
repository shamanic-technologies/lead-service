import "./instrument.js";
import * as Sentry from "@sentry/node";
import express from "express";
import cors from "cors";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import { db } from "./db/index.js";
import healthRoutes from "./routes/health.js";
import bufferRoutes from "./routes/buffer.js";
import cursorRoutes from "./routes/cursor.js";
import enrichRoutes from "./routes/enrich.js";
import leadsRoutes from "./routes/leads.js";

const app = express();
const PORT = process.env.PORT || 3006;

app.use(cors());
app.use(express.json());

app.use(healthRoutes);
app.use(bufferRoutes);
app.use(cursorRoutes);
app.use(enrichRoutes);
app.use(leadsRoutes);

app.use((req, res) => {
  res.status(404).json({ error: "Not found" });
});

Sentry.setupExpressErrorHandler(app);

app.use((err: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error("Unhandled error:", err);
  res.status(500).json({ error: "Internal server error" });
});

if (process.env.NODE_ENV !== "test") {
  migrate(db, { migrationsFolder: "./drizzle" })
    .then(() => {
      console.log("Migrations complete");
      app.listen(Number(PORT), "::", () => {
        console.log(`lead-service running on port ${PORT}`);
      });
    })
    .catch((err) => {
      console.error("Migration failed:", err);
      process.exit(1);
    });
}

export default app;
