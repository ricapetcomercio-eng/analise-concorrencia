// api/concorrentes.js
//
// Variáveis de ambiente necessárias no Vercel (Settings > Environment Variables):
//   TURSO_DATABASE_URL          -> URL do banco Turso
//   TURSO_AUTH_TOKEN            -> token de autenticação do Turso
//   CONCORRENTES_WEBHOOK_SECRET -> chave que o scraper local usa pra autenticar
//
// Migrado de Redis (Upstash, compartilhado com outros projetos — Painel
// Entrega Turbo e painelvendas-seven — e que por isso estourava a cota de
// 500 mil requisições/mês) para Turso (SQLite dedicado só a este projeto).
//
// Rotas (inalteradas — só a forma de armazenamento mudou):
//   POST /api/concorrentes                          -> recebe o payload do scraper local e salva
//   GET  /api/concorrentes                           -> devolve o último snapshot salvo
//   GET  /api/concorrentes?data=YYYY-MM-DD           -> devolve o snapshot daquele dia
//   GET  /api/concorrentes?action=historico          -> posição por subcategoria ao longo do tempo
//   GET  /api/concorrentes?action=historico_concorrentes -> métricas por concorrente/produto ao longo do tempo

const { getDb } = require('../lib/db');

// Cria as tabelas se ainda não existirem — barato o suficiente (IF NOT
// EXISTS) para rodar em toda chamada, sem precisar de um passo de setup
// separado.
async function garantirTabelas(db) {
  await db.execute(`CREATE TABLE IF NOT EXISTS concorrentes_historico (
    data TEXT PRIMARY KEY,
    payload TEXT,
    atualizado_em INTEGER
  )`);
  await db.execute(`CREATE TABLE IF NOT EXISTS concorrentes_kv (
    chave TEXT PRIMARY KEY,
    valor TEXT,
    atualizado_em INTEGER
  )`);
}

async function salvarSnapshotDia(db, dataDia, payloadStr) {
  await db.execute({
    sql: `INSERT INTO concorrentes_historico (data, payload, atualizado_em) VALUES (?, ?, ?)
          ON CONFLICT(data) DO UPDATE SET payload = excluded.payload, atualizado_em = excluded.atualizado_em`,
    args: [dataDia, payloadStr, Date.now()],
  });
}

async function salvarKV(db, chave, valor) {
  await db.execute({
    sql: `INSERT INTO concorrentes_kv (chave, valor, atualizado_em) VALUES (?, ?, ?)
          ON CONFLICT(chave) DO UPDATE SET valor = excluded.valor, atualizado_em = excluded.atualizado_em`,
    args: [chave, valor, Date.now()],
  });
}

async function buscarKV(db, chave) {
  const rs = await db.execute({ sql: 'SELECT valor FROM concorrentes_kv WHERE chave = ?', args: [chave] });
  return rs.rows[0] ? rs.rows[0].valor : null;
}

async function buscarSnapshotDia(db, dataDia) {
  const rs = await db.execute({ sql: 'SELECT payload FROM concorrentes_historico WHERE data = ?', args: [dataDia] });
  return rs.rows[0] ? rs.rows[0].payload : null;
}

async function listarDatas(db) {
  const rs = await db.execute('SELECT data FROM concorrentes_historico ORDER BY data ASC');
  return rs.rows.map((r) => r.data);
}

module.exports = async function handler(req, res) {
  const db = getDb();
  await garantirTabelas(db);

  if (req.method === 'POST') {
    const authHeader = req.headers['authorization'] || '';
    const expected = `Bearer ${process.env.CONCORRENTES_WEBHOOK_SECRET}`;
    if (authHeader !== expected) {
      return res.status(401).json({ error: 'não autorizado' });
    }

    const payload = req.body;
    if (!payload || !payload.categorias) {
      return res.status(400).json({ error: 'payload inválido' });
    }

    const dataStr = JSON.stringify(payload);
    await salvarKV(db, 'ultimo_snapshot', dataStr);

    const dataDia = payload.coletado_em.slice(0, 10);
    await salvarSnapshotDia(db, dataDia, dataStr);

    return res.status(200).json({ ok: true, subcategorias: payload.categorias.length });
  }

  if (req.method === 'GET') {
    const { data, action } = req.query;

    if (action === 'historico') {
      const datas = await listarDatas(db);

      // subcategoriaMap[subcategoria][vendedor][data] = posicao
      const subcategoriaMap = {};

      for (const dia of datas) {
        const raw = await buscarSnapshotDia(db, dia);
        if (!raw) continue;
        const payload = JSON.parse(raw);

        for (const cat of payload.categorias || []) {
          const subNome = cat.subcategoria || '(padrão)';
          if (!subcategoriaMap[subNome]) subcategoriaMap[subNome] = {};

          for (const linha of cat.sua_posicao || []) {
            const chaves = Object.keys(linha);
            const vendedorKey = chaves.find((k) => k.toLowerCase().includes('vendedor'));
            const posicaoKey = chaves.find((k) => k.toLowerCase().includes('posi'));
            const vendedor = vendedorKey ? linha[vendedorKey] : null;
            const posicao = posicaoKey ? linha[posicaoKey] : null;
            if (!vendedor) continue;

            if (!subcategoriaMap[subNome][vendedor]) subcategoriaMap[subNome][vendedor] = {};
            subcategoriaMap[subNome][vendedor][dia] = posicao;
          }
        }
      }

      return res.status(200).json({ datas, subcategorias: subcategoriaMap });
    }

    if (action === 'historico_concorrentes') {
      const datas = await listarDatas(db);

      // concorrentesMap[nomeConcorrente][nomeProduto][data] = { vendas_brutas, quantidade_de_vendas, visitas, conversao, posicao }
      const concorrentesMap = {};

      for (const dia of datas) {
        const raw = await buscarSnapshotDia(db, dia);
        if (!raw) continue;
        const payload = JSON.parse(raw);

        for (const comp of payload.concorrentes || []) {
          if (!concorrentesMap[comp.nome]) concorrentesMap[comp.nome] = {};

          // Alguns concorrentes têm múltiplos anúncios com o MESMO nome
          // (ex: variações de catálogo do Mercado Livre). Sem esse contador,
          // o segundo anúncio com nome repetido sobrescrevia o primeiro no
          // mesmo dia, fazendo produtos "sumirem" do painel.
          const contagemNomes = {};

          (comp.produtos || []).forEach((linha, idx) => {
            const chaves = Object.keys(linha);
            const anuncioKey =
              chaves.find((k) => k.toLowerCase().includes('núncio')) ||
              chaves.find((k) => k.toLowerCase().includes('anuncio')) ||
              chaves[0];
            const nomeProduto = anuncioKey ? linha[anuncioKey] : null;
            if (!nomeProduto) return;

            contagemNomes[nomeProduto] = (contagemNomes[nomeProduto] || 0) + 1;
            const ocorrencia = contagemNomes[nomeProduto];
            const nomeChave = ocorrencia > 1 ? `${nomeProduto} (${ocorrencia})` : nomeProduto;

            const vendasKey = chaves.find((k) => k.toLowerCase().includes('venda') && k.toLowerCase().includes('brut'));
            const qtdKey = chaves.find((k) => k.toLowerCase().includes('quantidade'));
            const visitasKey = chaves.find((k) => k.toLowerCase().includes('visita'));
            const conversaoKey = chaves.find((k) => k.toLowerCase().includes('convers'));

            if (!concorrentesMap[comp.nome][nomeChave]) {
              concorrentesMap[comp.nome][nomeChave] = {};
            }
            concorrentesMap[comp.nome][nomeChave][dia] = {
              posicao: idx + 1,
              vendas_brutas: vendasKey ? linha[vendasKey] : null,
              quantidade_de_vendas: qtdKey ? linha[qtdKey] : null,
              visitas: visitasKey ? linha[visitasKey] : null,
              conversao: conversaoKey ? linha[conversaoKey] : null,
              imagem: linha._imagem || null,
            };
          });
        }
      }

      return res.status(200).json({ datas, concorrentes: concorrentesMap });
    }

    if (data) {
      const raw = await buscarSnapshotDia(db, data);
      if (!raw) return res.status(404).json({ error: 'sem dados para essa data' });
      return res.status(200).json(JSON.parse(raw));
    }

    const raw = await buscarKV(db, 'ultimo_snapshot');
    if (!raw) return res.status(404).json({ error: 'nenhum snapshot ainda' });
    return res.status(200).json(JSON.parse(raw));
  }

  return res.status(405).json({ error: 'método não suportado' });
};
