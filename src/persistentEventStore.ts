// src/persistentEventStore.ts
import { JSONRPCMessage } from '@modelcontextprotocol/sdk/types';
import { EventStore, EventId, StreamId } from '@modelcontextprotocol/sdk/server/streamableHttp';
import sql, { ConnectionPool, Transaction, NVarChar, DateTime } from 'mssql'; // Microsoft SQL Server client
import { randomUUID } from 'node:crypto';

// --- Database Configuration ---
// IMPORTANT: Store these securely, e.g., in Azure App Service Configuration (Environment Variables)
const dbConfig: sql.config = {
  user: process.env.DB_USER,          // e.g., 'your_sql_user'
  password: process.env.DB_PASSWORD,  // e.g., 'your_sql_password'
  server: process.env.DB_SERVER!,     // e.g., 'your-server-name.database.windows.net'
  database: process.env.DB_NAME,      // e.g., 'your_database_name'
  options: {
    encrypt: true, // For Azure SQL, encryption is required
    trustServerCertificate: false, // Change to true for local dev / self-signed certs
    // connectionTimeout: 30000, // Optional: 30 seconds
    // requestTimeout: 30000, // Optional: 30 seconds
  },
  pool: { // Optional: configure connection pool
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

let pool: ConnectionPool | null = null;

async function getDbPool(): Promise<ConnectionPool> {
  if (!pool) {
    try {
      console.log('PersistentEventStore: Creating new SQL Server connection pool...');
      pool = new ConnectionPool(dbConfig);
      await pool.connect();
      console.log('PersistentEventStore: SQL Server connection pool connected.');

      pool.on('error', err => {
        console.error('PersistentEventStore: SQL Server connection pool error:', err);
        // Optionally try to reconnect or handle error
        pool = null; // Reset pool so it can be re-established
      });

    } catch (err) {
      console.error('PersistentEventStore: Database Connection Failed:', err);
      pool = null; // Ensure pool is null if connection fails
      throw err; // Re-throw error to indicate failure
    }
  }
  return pool;
}

// Call on application startup
getDbPool().catch(err => {
    console.error("Failed to connect to database on startup:", err);
    // Potentially exit or retry based on your application's needs
});


export class PersistentEventStore implements EventStore {
  constructor() {
    // The constructor can be light if the pool is managed globally
    // You could call ensureSchema here if you want it to run on instantiation
    // this.ensureSchema().catch(console.error);
  }

  // Optional: Method to ensure the table exists.
  // Call this once during application startup if you want the app to create the table.
  public async ensureSchema(): Promise<void> {
    try {
      const currentPool = await getDbPool();
      const request = currentPool.request();
      // This is a simplified check. A more robust check would query INFORMATION_SCHEMA.TABLES
      await request.query(`
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='McpEvents' and xtype='U')
        BEGIN
            CREATE TABLE McpEvents (
                EventId NVARCHAR(255) NOT NULL PRIMARY KEY,
                StreamId NVARCHAR(255) NOT NULL,
                MessageJson NVARCHAR(MAX) NOT NULL,
                StoredAt DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME()
            );

            CREATE INDEX IX_McpEvents_StreamId_StoredAt ON McpEvents (StreamId, StoredAt);
            PRINT 'McpEvents table created.';
        END
        ELSE
        BEGIN
            PRINT 'McpEvents table already exists.';
        END
      `);
      console.log("PersistentEventStore: Schema ensured (McpEvents table).");
    } catch (error) {
      console.error("PersistentEventStore: Error ensuring schema:", error);
      throw error;
    }
  }

  private generateEventId(streamId: StreamId): EventId {
    // Format: streamId_timestampMillis_randomSuffix
    // This makes EventId somewhat sortable by time if needed, though StoredAt is primary for sorting.
    return `${streamId}_${Date.now()}_${randomUUID().substring(0, 8)}`;
  }

  async storeEvent(streamId: StreamId, message: JSONRPCMessage): Promise<EventId> {
    const eventId = this.generateEventId(streamId);
    const messageJson = JSON.stringify(message);
    const storedAt = new Date(); // Use JS Date, mssql driver handles conversion

    try {
      const currentPool = await getDbPool();
      const request = currentPool.request();
      request.input('EventId', NVarChar(255), eventId);
      request.input('StreamId', NVarChar(255), streamId);
      request.input('MessageJson', NVarChar(sql.MAX), messageJson); // Use sql.MAX for NVARCHAR(MAX)
      request.input('StoredAt', DateTime, storedAt); // mssql driver handles JS Date to DATETIME2

      await request.query(
        'INSERT INTO McpEvents (EventId, StreamId, MessageJson, StoredAt) VALUES (@EventId, @StreamId, @MessageJson, @StoredAt)'
      );

      console.log(`PersistentEventStore: Stored event ${eventId} for stream ${streamId}`);
      return eventId;
    } catch (error) {
      console.error(`PersistentEventStore: Error storing event ${eventId}:`, error);
      throw error; // Re-throw to allow caller to handle
    }
  }

  async replayEventsAfter(
    lastEventId: EventId,
    { send }: { send: (eventId: EventId, message: JSONRPCMessage) => Promise<void> }
  ): Promise<StreamId> {
    if (!lastEventId) {
      console.warn("PersistentEventStore: replayEventsAfter called with no lastEventId.");
      return ''; // Or handle as an error. SDK might send empty if it's a fresh stream.
    }

    const streamId = lastEventId.split('_')[0]; // Assuming EventId format: streamId_timestamp_random
    if (!streamId) {
        console.error(`PersistentEventStore: Could not extract StreamId from lastEventId: ${lastEventId}`);
        return '';
    }

    console.log(`PersistentEventStore: Replaying events for stream ${streamId} after event ${lastEventId}`);

    try {
      const currentPool = await getDbPool();
      const request = currentPool.request();
      request.input('TargetStreamId', NVarChar(255), streamId);
      request.input('LastEventId', NVarChar(255), lastEventId);

      // Query to get events for the stream that were stored after the StoredAt time of the lastEventId
      const query = `
        SELECT e.EventId, e.MessageJson
        FROM McpEvents e
        WHERE e.StreamId = @TargetStreamId
          AND e.StoredAt > (SELECT le.StoredAt FROM McpEvents le WHERE le.EventId = @LastEventId AND le.StreamId = @TargetStreamId)
        ORDER BY e.StoredAt ASC;
      `;

      const result = await request.query(query);
      let replayedCount = 0;

      if (result.recordset && result.recordset.length > 0) {
        for (const row of result.recordset) {
          try {
            const message = JSON.parse(row.MessageJson) as JSONRPCMessage;
            await send(row.EventId, message);
            replayedCount++;
          } catch (parseError) {
            console.error(`PersistentEventStore: Error parsing replayed event ${row.EventId}:`, parseError);
            // Optionally skip this event or stop replay
          }
        }
      }
      console.log(`PersistentEventStore: Replayed ${replayedCount} events for stream ${streamId}`);
      return streamId;
    } catch (error) {
      console.error(`PersistentEventStore: Error replaying events for stream ${streamId}:`, error);
      throw error; // Re-throw to allow caller to handle
    }
  }
}

// Optional: Export an instance if you prefer a singleton pattern for the store
// However, managing the DB pool connection might be better done at the application level.
// export const persistentEventStore = new PersistentEventStore();

// Graceful shutdown for the database pool
async function closeDbPool() {
  if (pool) {
    console.log('PersistentEventStore: Closing SQL Server connection pool...');
    await pool.close();
    pool = null;
    console.log('PersistentEventStore: SQL Server connection pool closed.');
  }
}

process.on('SIGINT', async () => {
  await closeDbPool();
  process.exit(0);
});
process.on('SIGTERM', async () => {
  await closeDbPool();
  process.exit(0);
});
