// api/concorrentes.js
//
// Variáveis de ambiente necessárias no Vercel (Settings > Environment Variables):
//   REDIS_URL                   -> URL de conexão do Redis (ex: Upstash)
//   CONCORRENTES_WEBHOOK_SECRET -> chave que o scraper local usa pra autenticar
//
// Rotas:
//   POST /api/concorrentes          -> recebe o payload do scraper local e salva
//   GET  /api/concorrentes          -> devolve o último snapshot salvo
//   GET  /api/concorrentes?data=YYYY-MM-DD -> devolve o snapshot daquele dia

const { createClient } = require("redis");

const REDIS_KEY = "concorrentes:ultimo_snapshot";
const REDIS_HISTORICO_PREFIX = "concorrentes:historico:";
const REDIS_DATAS_SET = "concorrentes:datas";

let redisClient;
async function getRedis() {
  if (!redisClient) {
    redisClient = createClient({ url: process.env.REDIS_URL });
    redisClient.on("error", (err) => console.error("Redis error:", err));
    await redisClient.connect();
  }
  return redisClient;
}

module.exports = async function handler(req, res) {
  const redis = await getRedis();

  if (req.method === "POST") {
    const authHeader = req.headers["authorization"] || "";
    const expected = `Bearer ${process.env.CONCORRENTES_WEBHOOK_SECRET}`;
    if (authHeader !== expected) {
      return res.status(401).json({ error: "não autorizado" });
    }

    const payload = req.body;
    if (!payload || !payload.categorias) {
      return res.status(400).json({ error: "payload inválido" });
    }

    const dataStr = JSON.stringify(payload);
    await redis.set(REDIS_KEY, dataStr);

    const dataDia = payload.coletado_em.slice(0, 10);
    const diaKey = REDIS_HISTORICO_PREFIX + dataDia;
    await redis.set(diaKey, dataStr);
    await redis.sAdd(REDIS_DATAS_SET, dataDia);

    return res.status(200).json({ ok: true, subcategorias: payload.categorias.length });
  }

  if (req.method === "GET") {
    const { data, action } = req.query;

    if (action === "historico") {
      const datas = await redis.sMembers(REDIS_DATAS_SET);
      datas.sort();

      // subcategoriaMap[subcategoria][vendedor][data] = posicao
      const subcategoriaMap = {};

      for (const dia of datas) {
        const raw = await redis.get(REDIS_HISTORICO_PREFIX + dia);
        if (!raw) continue;
        const payload = JSON.parse(raw);

        for (const cat of payload.categorias || []) {
          const subNome = cat.subcategoria || "(padrão)";
          if (!subcategoriaMap[subNome]) subcategoriaMap[subNome] = {};

          for (const linha of cat.sua_posicao || []) {
            const chaves = Object.keys(linha);
            const vendedorKey = chaves.find((k) => k.toLowerCase().includes("vendedor"));
            const posicaoKey = chaves.find((k) => k.toLowerCase().includes("posi"));
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

    if (action === "historico_concorrentes") {
      const datas = await redis.sMembers(REDIS_DATAS_SET);
      datas.sort();

      // concorrentesMap[nomeConcorrente][nomeProduto][data] = { vendas_brutas, quantidade_de_vendas, visitas, conversao, posicao }
      const concorrentesMap = {};

      for (const dia of datas) {
        const raw = await redis.get(REDIS_HISTORICO_PREFIX + dia);
        if (!raw) continue;
        const payload = JSON.parse(raw);

        for (const comp of payload.concorrentes || []) {
          if (!concorrentesMap[comp.nome]) concorrentesMap[comp.nome] = {};

          (comp.produtos || []).forEach((linha, idx) => {
            const chaves = Object.keys(linha);
            const anuncioKey =
              chaves.find((k) => k.toLowerCase().includes("núncio")) ||
              chaves.find((k) => k.toLowerCase().includes("anuncio")) ||
              chaves[0];
            const nomeProduto = anuncioKey ? linha[anuncioKey] : null;
            if (!nomeProduto) return;

            const vendasKey = chaves.find((k) => k.toLowerCase().includes("venda") && k.toLowerCase().includes("brut"));
            const qtdKey = chaves.find((k) => k.toLowerCase().includes("quantidade"));
            const visitasKey = chaves.find((k) => k.toLowerCase().includes("visita"));
            const conversaoKey = chaves.find((k) => k.toLowerCase().includes("convers"));

            if (!concorrentesMap[comp.nome][nomeProduto]) {
              concorrentesMap[comp.nome][nomeProduto] = {};
            }
            concorrentesMap[comp.nome][nomeProduto][dia] = {
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
      const raw = await redis.get(REDIS_HISTORICO_PREFIX + data);
      if (!raw) return res.status(404).json({ error: "sem dados para essa data" });
      return res.status(200).json(JSON.parse(raw));
    }

    const raw = await redis.get(REDIS_KEY);
    if (!raw) return res.status(404).json({ error: "nenhum snapshot ainda" });
    return res.status(200).json(JSON.parse(raw));
  }

  return res.status(405).json({ error: "método não suportado" });
}
