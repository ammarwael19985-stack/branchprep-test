const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

let ordersData = {};

io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  // إرسال البيانات الكلية عند أول اتصال فقط
  socket.emit('init-data', ordersData);

  // تحديث جزئي سريع للأصناف والفروع (خفيف جداً ولا يسبب أي تهنيج)
  socket.on('update-item', ({ orderCode, barcode, branch, actual, worker }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      if (!item.branches) item.branches = {};
      if (!item.branches[branch]) item.branches[branch] = { target: 0, actual: "", worker: "" };

      item.branches[branch].actual = actual;
      item.branches[branch].worker = worker;
      ordersData[orderCode]._lastUpdated = Date.now();

      // إرسال التحديث الجزئي فقط لجميع الأجهزة فوراً
      io.emit('item-updated', { orderCode, barcode, branch, actual, worker });
    }
  });

  // تحديث الجلسات النشطة للموظفين فقط (خفيف وبدون إعادة رسم الشاشة)
  socket.on('worker-heartbeat', ({ orderCode, workerName }) => {
    if (ordersData[orderCode]) {
      if (!ordersData[orderCode].activeSessions) ordersData[orderCode].activeSessions = {};
      const cleanKey = workerName.replace(/[.#$\[\]]/g, "_");
      ordersData[orderCode].activeSessions[cleanKey] = { name: workerName, timestamp: Date.now() };
      
      io.emit('sessions-updated', { orderCode, activeSessions: ordersData[orderCode].activeSessions });
    }
  });

  // التحديث الكلي (يستخدم عند رفع طلبية جديدة من الإكسيل فقط)
  socket.on('update-data', (data) => {
    ordersData = data;
    io.emit('data-changed', ordersData);
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

app.get('/', (req, res) => {
  res.send('BranchPrep Fast Server is Running Successfully!');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
