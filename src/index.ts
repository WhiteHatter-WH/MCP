import express from 'express';
import { randomUUID } from 'node:crypto';
import { McpServer, ResourceTemplate } from '@modelcontextprotocol/sdk/server/mcp';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types';
import { z } from 'zod';
// Import the PersistentEventStore
import { PersistentEventStore } from './persistentEventStore.js'; //
// If you initialize and export an instance from persistentEventStore.ts:
// import { persistentEventStore } from './persistentEventStore.js';


const app = express();
app.use(express.json());

const mcpServer = new McpServer({
  name: "MyPersistentMCPServer",
  version: "1.0.0"
});

mcpServer.tool(
  "add",
  { a: z.number(), b: z.number() },
  async ({ a, b }) => ({
    content: [{ type: "text", text: String(a + b) }]
  })
);

mcpServer.resource(
  "greeting",
  new ResourceTemplate("greeting://{name}", { list: undefined }),
  async (uri, { name }) => ({
    contents: [{
      uri: uri.href,
      text: `Hello, ${name} from your persistent stateful server!`
    }]
  })
);

const transports: { [sessionId: string]: StreamableHTTPServerTransport } = {};
// If you instantiate PersistentEventStore per session (less common for a shared DB):
// const eventStores: { [sessionId: string]: PersistentEventStore } = {};

// Or, if you have a single shared persistentEventStore instance:
// (Ensure your dbClient in persistentEventStore.ts is initialized appropriately)
// const globalPersistentEventStore = new PersistentEventStore( /* your initialized dbClient */ );
// For this example, let's assume you might want a store per session for some reason,
// or you would pass a global one. For true persistence, a single client to the DB is typical.
// Reverting to the simpler per-session instantiation for this example, but highlighting that
// a single db client instance is usually preferred.
const getEventStoreForSession = (sessionId: string) => {
  // In a real scenario with a shared DB, you'd likely use one global PersistentEventStore instance
  // that connects to your database. For this example, to keep it contained,
  // we'll create a new one, but this means it would try to manage its own DB connections
  // if not designed carefully.
  // IDEALLY: const globalPersistentEventStore = new PersistentEventStore(yourActualDbClient);
  // and then just use globalPersistentEventStore.
  // For now, we'll follow the previous pattern conceptually:
  // This is where you'd inject your actual database client to the PersistentEventStore
  const placeholderDbClient: any = {
    query: async (sql: string, params: any[]) => { console.warn("Conceptual DB Query:", sql, params); return []; },
    execute: async (sql: string, params: any[]) => { console.warn("Conceptual DB Execute:", sql, params); },
  };
  return new PersistentEventStore(placeholderDbClient);
};


app.post('/mcp', async (req, res) => {
  const clientIp = req.ip;
  console.log(`[${clientIp}] Received POST /mcp request`);
  // ... (logging as before)

  const sessionId = req.headers['mcp-session-id'] as string | undefined;
  let transport: StreamableHTTPServerTransport;

  if (sessionId && transports[sessionId]) {
    console.log(`[${clientIp}] Reusing existing transport for session ID: ${sessionId}`);
    transport = transports[sessionId];
  } else if (!sessionId && isInitializeRequest(req.body)) {
    console.log(`[${clientIp}] New initialization request. Creating new transport.`);
    const newSessionId = randomUUID(); // Server generates session ID

    // Use PersistentEventStore
    const eventStore = getEventStoreForSession(newSessionId); // Or use a global instance

    transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => newSessionId,
      eventStore, // Use the persistent event store
      onsessioninitialized: (initializedSessionId) => {
        console.log(`[${clientIp}] Session initialized with ID: ${initializedSessionId}. Storing transport.`);
        transports[initializedSessionId] = transport;
        // If managing stores per session: eventStores[initializedSessionId] = eventStore;
      }
    });

    transport.onclose = () => {
      if (transport.sessionId) {
        console.log(`[${clientIp}] Transport closed for session ID: ${transport.sessionId}. Cleaning up.`);
        delete transports[transport.sessionId];
        // If managing stores per session: delete eventStores[transport.sessionId];
      }
    };

    console.log(`[${clientIp}] Connecting new transport to McpServer.`);
    await mcpServer.connect(transport);

  } else {
    console.log(`[${clientIp}] Invalid request: No valid session ID provided or not an initialization request.`);
    res.status(400).json({
      jsonrpc: '2.0',
      error: {
        code: -32000,
        message: 'Bad Request: No valid session ID provided or not an initialization request.',
      },
      id: req.body?.id || null,
    });
    return;
  }

  try {
    console.log(`[${clientIp}] Handling request with transport for session ID: ${transport.sessionId || 'new session'}`);
    await transport.handleRequest(req, res, req.body);
    console.log(`[${clientIp}] Request handling completed for session ID: ${transport.sessionId}`);
  } catch (error) {
    console.error(`[${clientIp}] Error during handleRequest for session ID ${transport.sessionId}:`, error);
    // ... (error response as before)
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32603, message: 'Internal Server Error' },
        id: req.body?.id || null
      });
    }
  }
});

const handleSessionRequest = async (req: express.Request, res: express.Response) => {
  // ... (same as before, ensures transport is fetched from `transports` map)
  const clientIp = req.ip;
  console.log(`[${clientIp}] Received ${req.method} /mcp request`);
  // ... (logging)

  const sessionId = req.headers['mcp-session-id'] as string | undefined;
  if (!sessionId || !transports[sessionId]) {
    console.log(`[${clientIp}] Invalid or missing session ID for ${req.method} request.`);
    res.status(400).send('Invalid or missing session ID');
    return;
  }
  
  const transport = transports[sessionId];
  console.log(`[${clientIp}] Handling ${req.method} request with transport for session ID: ${sessionId}`);
  try {
    await transport.handleRequest(req, res);
    console.log(`[${clientIp}] ${req.method} request handling completed for session ID: ${sessionId}`);
  } catch (error) {
     console.error(`[${clientIp}] Error during ${req.method} handleRequest for session ID ${sessionId}:`, error);
     if (!res.headersSent) {
      res.status(500).send('Internal Server Error');
    }
  }
};

app.get('/mcp', handleSessionRequest);
app.delete('/mcp', handleSessionRequest);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Persistent Stateful MCP Streamable HTTP Server listening on port ${PORT}`);
  console.log(`MCP endpoint: http://localhost:${PORT}/mcp`);
});