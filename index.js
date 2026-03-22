const { LogicalReplicationService, PgoutputPlugin } = require('pg-logical-replication');
const { Client } = require('pg');
const { MeiliSearch } = require('meilisearch');
const { createClient } = require('redis');
const fs = require('fs');
const path = require('path');
const { faker } = require('@faker-js/faker');

const PG_CONFIG = {
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
  database: process.env.POSTGRES_DB || 'cdc_demo',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres',
};

const LSN_FILE = path.join(__appDataDir(), '..', 'data', 'lsn_checkpoint.txt');

function __appDataDir() {
    return __dirname;
}

const meiliClient = new MeiliSearch({
  host: process.env.MEILI_HOST || 'http://localhost:7700',
  apiKey: process.env.MEILI_MASTER_KEY || 'masterKey123!',
});

const redisClient = createClient({
  url: process.env.REDIS_URL || 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.log('Redis Client Error', err));

async function initializeIndexes() {
  const index = meiliClient.index('products');
  await index.updateFilterableAttributes(['category', 'in_stock']);
  await index.updateSortableAttributes(['price']);
  console.log('Meilisearch indexes initialized.');
}

async function seedDatabaseIfEmpty() {
  const client = new Client(PG_CONFIG);
  await client.connect();
  const res = await client.query('SELECT COUNT(*) FROM products');
  if (parseInt(res.rows[0].count, 10) === 0) {
    console.log('Seeding database with Faker...');
    try {
      await client.query('BEGIN');
      // Create categories
      const categories = ['Electronics', 'Clothing', 'Home', 'Books', 'Toys'];
      for (const cat of categories) {
        await client.query('INSERT INTO categories (name) VALUES ($1)', [cat]);
      }
      
      const catRes = await client.query('SELECT category_id, name FROM categories');
      const categoryMap = catRes.rows;
      
      // Create products
      for (let i = 0; i < 1000; i++) {
        const cat = categoryMap[Math.floor(Math.random() * categoryMap.length)];
        const insertProduct = `
          INSERT INTO products (name, description, price, category_id)
          VALUES ($1, $2, $3, $4) RETURNING product_id
        `;
        const pRes = await client.query(insertProduct, [
          faker.commerce.productName(),
          faker.commerce.productDescription(),
          faker.commerce.price({ min: 10, max: 1000 }),
          cat.category_id
        ]);
        const productId = pRes.rows[0].product_id;
        
        // Create inventory
        const insertInventory = `
          INSERT INTO inventory (product_id, quantity)
          VALUES ($1, $2)
        `;
        await client.query(insertInventory, [productId, faker.number.int({ min: 0, max: 100 })]);
      }
      await client.query('COMMIT');
      console.log('Database seeded with 1000 products.');
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('Seeding failed', e);
    }
  }
  await client.end();
}

function getSavedLSN() {
  const lsnPath = path.join(__dirname, 'data', 'lsn_checkpoint.txt');
  if (fs.existsSync(lsnPath)) {
    return fs.readFileSync(lsnPath, 'utf8').trim();
  }
  return null;
}

function saveLSN(lsn) {
  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  fs.writeFileSync(path.join(dataDir, 'lsn_checkpoint.txt'), lsn);
}

// Global state to hold relation mappings (table names and schemas)
const relations = {};
let currentTransactionLsn = null;

async function fetchEnrichedProduct(productId) {
  const client = new Client(PG_CONFIG);
  await client.connect();
  const query = `
    SELECT 
      p.product_id as id,
      p.name,
      p.description,
      p.price::float as price,
      c.name as category,
      COALESCE(i.quantity, 0) > 0 as in_stock,
      COALESCE(i.quantity, 0) as quantity
    FROM products p
    LEFT JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN inventory i ON p.product_id = i.product_id
    WHERE p.product_id = $1
  `;
  const res = await client.query(query, [productId]);
  await client.end();
  return res.rows[0];
}

async function handleAction(action, tableName, row) {
  let productId = null;
  
  if (tableName === 'products') productId = row.product_id;
  if (tableName === 'inventory') productId = row.product_id;
  // If categories changes, we might want to update all products in that category, but skipping for simplicity
  
  if (!productId) return;

  const eventPayload = {
    table: tableName,
    operation: action,
    timestamp: new Date().toISOString(),
    data: row
  };
  
  console.log(`Sending to Redis: ${JSON.stringify(eventPayload)}`);
  await redisClient.publish('cdc_events', JSON.stringify(eventPayload));

  if (action === 'DELETE' && tableName === 'products') {
    await meiliClient.index('products').deleteDocument(productId);
  } else {
    const enriched = await fetchEnrichedProduct(productId);
    if (enriched) {
      await meiliClient.index('products').addDocuments([enriched]);
    }
  }
}

async function main() {
  await redisClient.connect();
  
  const plugin = new LogicalReplicationService(PG_CONFIG, { acknowledge: { auto: true, timeoutSeconds: 10 } });
  
  await seedDatabaseIfEmpty();
  await initializeIndexes();

  plugin.on('data', async (lsn, log) => {
    currentTransactionLsn = lsn;
    // log object has different properties based on the message type
    if (log.tag === 'relation') {
      relations[log.relationId] = log;
    } else if (log.tag === 'insert') {
      const relation = relations[log.relationId];
      if (relation) {
        await handleAction('INSERT', relation.name, log.new);
      }
    } else if (log.tag === 'update') {
      const relation = relations[log.relationId];
      if (relation) {
        await handleAction('UPDATE', relation.name, log.new);
      }
    } else if (log.tag === 'delete') {
      const relation = relations[log.relationId];
      if (relation) {
        await handleAction('DELETE', relation.name, log.old || log.key);
      }
    } else if (log.tag === 'commit') {
      saveLSN(lsn);
    }
  });

  plugin.on('error', (err) => {
    console.error('Replication error:', err);
  });

  const slotName = 'my_replication_slot';
  const startLsn = getSavedLSN();
  
  console.log(`Starting replication on slot ${slotName} from LSN ${startLsn || '0/0'}`);
  
  try {
    const client = new Client(PG_CONFIG);
    await client.connect();
    // Create replication slot if it doesn't exist
    await client.query(`SELECT pg_create_logical_replication_slot('${slotName}', 'pgoutput')`).catch(e => {
        if(e.code !== '42710') console.log('Slot already exists or error:', e.message);
    });
    await client.end();
    
    await plugin.subscribe(new PgoutputPlugin({
      publicationNames: ['my_publication'],
      protoVersion: 1
    }), slotName, startLsn);
    console.log('Listening to logical replication stream...');
  } catch(e) {
      console.error(e);
  }
}

// wait for services
setTimeout(main, 5000);
