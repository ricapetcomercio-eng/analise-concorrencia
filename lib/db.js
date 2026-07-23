// lib/db.js
// Cliente SQLite (Turso/libSQL) — banco dedicado a este projeto, separado
// do Redis compartilhado que causava estouro de cota.
//
// Variáveis de ambiente necessárias no projeto Vercel:
//   TURSO_DATABASE_URL
//   TURSO_AUTH_TOKEN

const { createClient } = require('@libsql/client');

let client = null;

function getDb() {
  if (!client) {
    const url = process.env.TURSO_DATABASE_URL;
    const authToken = process.env.TURSO_AUTH_TOKEN;
    if (!url || !authToken) {
      throw new Error(
        'Faltam as variáveis de ambiente TURSO_DATABASE_URL e/ou TURSO_AUTH_TOKEN. ' +
        'Configure-as em Project Settings > Environment Variables na Vercel.'
      );
    }
    client = createClient({ url, authToken });
  }
  return client;
}

module.exports = { getDb };
