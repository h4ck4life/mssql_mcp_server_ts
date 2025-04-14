import { FastMCP, UserError } from "fastmcp";
import { z } from "zod";
import * as sql from "mssql";
import * as dotenv from "dotenv";

// Load environment variables
dotenv.config();

// Constants for timeout handling
const DEFAULT_QUERY_TIMEOUT = 120; // seconds
const DEFAULT_SSE_PORT = 8080; // Default port for SSE server

// Initialize server
const server = new FastMCP({
  name: "mssql_mcp_server",
  version: "1.0.0",
});

// Configure logging to use stderr for diagnostic messages
const logger = {
  info: (message: string, data?: any) =>
    console.error(`INFO: ${message}${data ? " " + JSON.stringify(data) : ""}`),
  error: (message: string, data?: any) =>
    console.error(`ERROR: ${message}${data ? " " + JSON.stringify(data) : ""}`),
  warning: (message: string, data?: any) =>
    console.error(`WARN: ${message}${data ? " " + JSON.stringify(data) : ""}`),
};

function getDbConfig() {
  const config = {
    driver: process.env.MSSQL_DRIVER || "SQL Server",
    server: process.env.MSSQL_HOST || "localhost",
    user: process.env.MSSQL_USER || "",
    password: process.env.MSSQL_PASSWORD || "",
    database: process.env.MSSQL_DATABASE || "",
    queryTimeout: parseInt(
      process.env.MSSQL_QUERY_TIMEOUT || String(DEFAULT_QUERY_TIMEOUT)
    ),
  };

  if (!config.user || !config.password || !config.database) {
    logger.error(
      "Missing required database configuration. Please check environment variables:"
    );
    logger.error("MSSQL_USER, MSSQL_PASSWORD, and MSSQL_DATABASE are required");
    throw new Error("Missing required database configuration");
  }

  return config;
}

function isWriteOperation(query: string): boolean {
  const normalizedQuery = query.trim().toUpperCase();

  // List of SQL commands that modify data or structure
  const writeOperations = [
    "CREATE",
    "ALTER",
    "DROP",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "MERGE",
    "UPSERT",
    "GRANT",
    "REVOKE",
    "EXEC",
    "EXECUTE",
  ];

  for (const operation of writeOperations) {
    if (
      normalizedQuery.startsWith(operation) ||
      normalizedQuery.includes(` ${operation} `)
    ) {
      return true;
    }
  }

  return false;
}

async function executeQuery(query: string, fetchResults = true): Promise<any> {
  const config = getDbConfig();

  // Create SQL connection configuration
  const sqlConfig: sql.config = {
    user: config.user,
    password: config.password,
    database: config.database,
    server: config.server,
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    },
    options: {
      encrypt: true,
      trustServerCertificate: true,
      requestTimeout: config.queryTimeout * 1000,
    },
  };

  try {
    // Create a new connection for each query
    const pool = await sql.connect(sqlConfig);

    try {
      if (fetchResults) {
        const result = await pool.request().query(query);
        const columns =
          result.recordset && result.recordset.columns
            ? Object.keys(result.recordset.columns)
            : [];
        return { columns, rows: result.recordset };
      } else {
        const result = await pool.request().query(query);
        return { columns: null, rowCount: result.rowsAffected[0] };
      }
    } finally {
      await pool.close();
    }
  } catch (e) {
    logger.error(`Error executing query: ${e}`);
    throw e;
  }
}

// Add tools to the server
server.addTool({
  name: "execute_sql",
  description:
    "Execute a read-only SQL query on the MSSQL server. Write operations (CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, etc.) are not permitted.",
  parameters: z.object({
    query: z
      .string()
      .describe("The SQL query to execute (read-only operations only)"),
  }),
  execute: async (args, { log }) => {
    const config = getDbConfig();
    logger.info(`Executing SQL query: ${args.query}`);

    if (!args.query) {
      throw new UserError("Query is required");
    }

    // Check if the query is a write operation
    if (isWriteOperation(args.query)) {
      const errorMessage =
        "Write operations (CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, etc.) are not permitted for security reasons.";
      logger.warning(
        `Attempted write operation denied: ${args.query.substring(0, 100)}...`
      );
      throw new UserError(errorMessage);
    }

    try {
      // Special handling for listing tables in MSSQL
      if (args.query.trim().toUpperCase() === "SHOW TABLES") {
        const { columns, rows } = await executeQuery(
          "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
        );

        const result = [`Tables_in_${config.database}`]; // Header
        rows.forEach((row: any) => {
          result.push(row.TABLE_NAME);
        });

        return result.join("\n");
      }

      // For all other queries, treat them as SELECT queries and return the results
      // This is safe because we've already checked that it's not a write operation
      const { columns, rows } = await executeQuery(args.query);

      if (!rows || rows.length === 0) {
        return "No results found";
      }

      // Format the results in a tabular format
      const headerRow = columns.join(",");
      const dataRows = rows.map((row: any) => {
        return columns
          .map((col) => {
            const value = row[col];
            return value !== undefined && value !== null ? String(value) : "";
          })
          .join(",");
      });

      return [headerRow, ...dataRows].join("\n");
    } catch (e: any) {
      const errorMessage = e.message || String(e);
      logger.error(`Error executing SQL '${args.query}': ${errorMessage}`);
      throw new UserError(`Error executing query: ${errorMessage}`);
    }
  },
});

// Add event listeners for session management
server.on("connect", (event) => {
  logger.info(`Client connected: ${event.session.id}`);
});

server.on("disconnect", (event) => {
  logger.info(`Client disconnected: ${event.session.id}`);
});

// Start the server with SSE transport
logger.info("Starting MSSQL MCP server with SSE...");
try {
  const config = getDbConfig();
  logger.info(
    `Database config: ${config.server}/${config.database} as ${config.user}`
  );

  // Get SSE port from environment or use default
  const ssePort = parseInt(process.env.SSE_PORT || String(DEFAULT_SSE_PORT));
  
  // Start the server with SSE transport
  server.start({
    transportType: "sse",
    sse: {
      endpoint: "/sse",
      port: ssePort,
    },
  });

  logger.info(`MSSQL MCP server started with SSE on port ${ssePort}`);
  logger.info(`Connect to http://localhost:${ssePort}/sse`);
} catch (error) {
  logger.error(`Startup error: ${error}`);
  process.exit(1);
}