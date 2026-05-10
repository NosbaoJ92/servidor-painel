require('dotenv').config();

const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);

// =========================================================
// CONFIGURAÇÃO DE CORS
// =========================================================
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',').map(origin => origin.trim())
  : ['http://localhost:3000'];

app.use(cors({
  origin: allowedOrigins,
  methods: ['GET', 'POST'],
  credentials: true,
}));

// =========================================================
// ROTA TESTE
// =========================================================
app.get('/', (req, res) => {
  res.json({
    status: 'online',
    socket: true,
    timestamp: new Date(),
  });
});

// =========================================================
// SOCKET.IO
// =========================================================
const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ['GET', 'POST'],
    credentials: true,
  },
  transports: ['websocket', 'polling'],
});

// =========================================================
// VARIÁVEIS GLOBAIS
// =========================================================
let normalCounter = 0;
let priorityCounter = 0;

let normalQueue = [];
let priorityQueue = [];

let lastCalledTickets = [];
let callHistory = [];

const MAX_PANEL_CALLS = 4;
const MAX_HISTORY = 50;

// =========================================================
// FUNÇÃO AUXILIAR
// =========================================================
function broadcastQueueState() {
  const waitingQueueCombined = [...priorityQueue, ...normalQueue];

  const state = {
    normalQueue,
    priorityQueue,
    lastCalledTickets,
    waitingQueue: waitingQueueCombined,
    callHistory,
  };

  io.emit('filas_atualizadas', state);

  console.log(
    `📡 Broadcast enviado | Espera: ${waitingQueueCombined.length}`
  );
}

// =========================================================
// RELATÓRIO
// =========================================================
function gerarRelatorioDoDia() {
  try {
    const agora = new Date();

    const dataFormatada = agora.toLocaleDateString('pt-BR');
    const hora = agora.toLocaleTimeString('pt-BR');

    const relatorio = {
      data: dataFormatada,
      horaGeracao: hora,
      totalNormal: normalQueue.length,
      totalPrioritaria: priorityQueue.length,
      totalHistorico: callHistory.length,
      historico: callHistory,
    };

    const pasta = path.join(__dirname, 'relatorios');

    if (!fs.existsSync(pasta)) {
      fs.mkdirSync(pasta);
    }

    const filePath = path.join(
      pasta,
      `relatorio-${dataFormatada.replace(/\//g, '-')}.json`
    );

    fs.writeFileSync(
      filePath,
      JSON.stringify(relatorio, null, 2),
      'utf-8'
    );

    console.log(`✅ Relatório salvo: ${filePath}`);
  } catch (err) {
    console.error('❌ Erro ao gerar relatório:', err.message);
  }
}

// =========================================================
// RESET DIÁRIO
// =========================================================
function resetarSistemaDiariamente() {
  console.log('🔄 Reset diário iniciado...');

  gerarRelatorioDoDia();

  normalCounter = 0;
  priorityCounter = 0;

  normalQueue = [];
  priorityQueue = [];

  lastCalledTickets = [];
  callHistory = [];

  broadcastQueueState();

  console.log('✅ Sistema resetado.');
}

let ultimoDia = new Date().getDate();

setInterval(() => {
  const hoje = new Date().getDate();

  if (hoje !== ultimoDia) {
    resetarSistemaDiariamente();
    ultimoDia = hoje;
  }
}, 60000);

// =========================================================
// SOCKET EVENTS
// =========================================================
io.on('connection', (socket) => {
  console.log(`🟢 Cliente conectado: ${socket.id}`);

  const waitingQueueCombined = [...priorityQueue, ...normalQueue];

  // =====================================================
  // ENVIA ESTADO INICIAL
  // =====================================================
  socket.emit('estado_inicial', {
    normalQueue,
    priorityQueue,
    waitingQueue: waitingQueueCombined,
    lastCalledTickets,
    callHistory,
  });

  // =====================================================
  // EMITIR SENHA
  // =====================================================
  socket.on('emitir_senha_usuario', (tipo, callback, origem = 'manual') => {
    try {
      if (tipo !== 'normal' && tipo !== 'prioritaria') {
        const error = 'Tipo inválido';

        if (typeof callback === 'function') {
          callback({
            success: false,
            error,
          });
        }

        return;
      }

      let ticket;

      if (tipo === 'normal') {
        normalCounter++;

        ticket = {
          tipo: 'N',
          numero: String(normalCounter).padStart(3, '0'),
          categoria: 'NORMAL',
          origem,
          hora: new Date().toLocaleTimeString('pt-BR'),
          data: new Date().toLocaleDateString('pt-BR'),
          timestamp: Date.now(),
        };

        normalQueue.push(ticket);
      } else {
        priorityCounter++;

        ticket = {
          tipo: 'P',
          numero: String(priorityCounter).padStart(3, '0'),
          categoria: 'PRIORITÁRIA',
          origem,
          hora: new Date().toLocaleTimeString('pt-BR'),
          data: new Date().toLocaleDateString('pt-BR'),
          timestamp: Date.now(),
        };

        priorityQueue.push(ticket);
      }

      console.log(`🎫 Senha emitida: ${ticket.tipo}${ticket.numero}`);

      broadcastQueueState();

      if (typeof callback === 'function') {
        callback({
          success: true,
          ticket,
        });
      }
    } catch (err) {
      console.error('❌ Erro emitir senha:', err.message);

      if (typeof callback === 'function') {
        callback({
          success: false,
          error: err.message,
        });
      }
    }
  });

  // =====================================================
  // CHAMAR SENHA
  // =====================================================
  socket.on('chamar_senha', (callInfo) => {
    try {
      const { tipo, numero, guiche } = callInfo;

      const currentCalled = {
        tipo,
        numero,
        guiche,
        timestamp: new Date().toISOString(),
      };

      lastCalledTickets.unshift(currentCalled);

      if (lastCalledTickets.length > MAX_PANEL_CALLS) {
        lastCalledTickets.pop();
      }

      io.emit('senha_chamada', {
        currentCalled,
      });

      console.log(
        `📢 Senha chamada: ${tipo}${numero} | Guichê ${guiche}`
      );
    } catch (err) {
      console.error('❌ Erro chamar senha:', err.message);
    }
  });

  // =====================================================
  // SINCRONIZAR FILAS
  // =====================================================
  socket.on('sincronizar_filas_apos_chamada', (data) => {
    try {
      normalQueue = Array.isArray(data.normalQueue)
        ? data.normalQueue
        : normalQueue;

      priorityQueue = Array.isArray(data.priorityQueue)
        ? data.priorityQueue
        : priorityQueue;

      broadcastQueueState();

      console.log('🔄 Filas sincronizadas.');
    } catch (err) {
      console.error('❌ Erro sincronizar filas:', err.message);
    }
  });

  // =====================================================
  // FINALIZAR ATENDIMENTO
  // =====================================================
  socket.on('finalizar_atendimento', (ticketInfo) => {
    try {
      const { guiche, tipo, numero } = ticketInfo;

      const newEntry = {
        guiche,
        tipo,
        numero,
        timestamp: new Date().toISOString(),
      };

      callHistory.unshift(newEntry);

      if (callHistory.length > MAX_HISTORY) {
        callHistory.pop();
      }

      io.emit('historico_adicionado', newEntry);

      console.log(
        `✅ Atendimento finalizado: ${tipo}${numero} | Guichê ${guiche}`
      );
    } catch (err) {
      console.error('❌ Erro finalizar atendimento:', err.message);
    }
  });

  // =====================================================
  // DESCONECTOU
  // =====================================================
  socket.on('disconnect', (reason) => {
    console.log(`🔴 Cliente desconectado: ${socket.id}`);
    console.log(`📌 Motivo: ${reason}`);
  });

  // =====================================================
  // ERRO SOCKET
  // =====================================================
  socket.on('error', (err) => {
    console.error(`❌ Socket erro: ${err.message}`);
  });
});

// =========================================================
// START SERVER
// =========================================================
const PORT = process.env.PORT || 3001;
const HOST = process.env.SERVER_HOST || '0.0.0.0';

server.listen(PORT, HOST, () => {
  console.log('=================================================');
  console.log(`✅ Servidor online`);
  console.log(`🌐 HOST: ${HOST}`);
  console.log(`🚪 PORTA: ${PORT}`);
  console.log(`🔗 URL: http://${HOST}:${PORT}`);
  console.log(`🟢 Socket.IO ativo`);
  console.log(`🌍 Origins permitidas:`);
  console.log(allowedOrigins);
  console.log('=================================================');
}); 