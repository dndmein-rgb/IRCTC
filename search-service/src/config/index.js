import packageJson from "../../package.json" with { type: "json" };

const KAFKA_BROKER = process.env.KAFKA_BROKER;

if (!KAFKA_BROKER) {
  throw new Error("KAFKA_BROKER is not defined");
}
export const config = {
     SERVICE_NAME: packageJson.name,
     PORT: Number(process.env.PORT) || 4002,
     NODE_ENV: process.env.NODE_ENV || "development",
     LOG_LEVEL: process.env.LOG_LEVEL || "info",
     ELASTICSEARCH_URL: process.env.ELASTICSEARCH_URL,
     KAFKA_BROKER,
     KAFKA_CLIENT_ID: process.env.KAFKA_CLIENT_ID,
     ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS,
}
