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

  // إرسال البيانات الحالية عند الاتصال
  socket.emit('init-data', ordersData);

  // 1. التحقق من صحة كود الطلبية قبل دخول الموظف
  socket.on('validate-order', ({ orderCode }, callback) => {
    const isValid = !!(ordersData && ordersData[orderCode]);
    if (typeof callback === 'function') {
      callback({ valid: isValid });
    }
  });

  // 2. طلب حجز/قفل صنف لموظف
  socket.on('acquire-lock', ({ orderCode, barcode, workerName, sessionId }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      const now = Date.now();

      if (!item.lock || item.lock.sessionId === sessionId || (now - (item.lock.timestamp || 0) > 15000)) {
        item.lock = { worker: workerName, sessionId, timestamp: now };
        io.emit('lock-changed', { orderCode, barcode, lock: item.lock });
      } else {
        socket.emit('lock-changed', { orderCode, barcode, lock: item.lock });
      }
    }
  });

  // تجديد وقت القفل
  socket.on('keep-lock-alive', ({ orderCode, barcode, sessionId }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      if (item.lock && item.lock.sessionId === sessionId) {
        item.lock.timestamp = Date.now();
        io.emit('lock-changed', { orderCode, barcode, lock: item.lock });
      }
    }
  });

  // فك القفل
  socket.on('release-lock', ({ orderCode, barcode, sessionId }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      if (item.lock && item.lock.sessionId === sessionId) {
        delete item.lock;
        io.emit('lock-changed', { orderCode, barcode, lock: null });
      }
    }
  });

  // تحديث جزئي سريع للأصناف والفروع
  socket.on('update-item', ({ orderCode, barcode, branch, actual, worker }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      if (!item.branches) item.branches = {};
      if (!item.branches[branch]) item.branches[branch] = { target: 0, actual: "", worker: "" };

      item.branches[branch].actual = actual;
      item.branches[branch].worker = worker;
      ordersData[orderCode]._lastUpdated = Date.now();

      io.emit('item-updated', { orderCode, barcode, branch, actual, worker });
    }
  });

  // تحديث نبض التواجد للموظف
  socket.on('worker-heartbeat', ({ orderCode, workerName }) => {
    if (ordersData[orderCode]) {
      if (!ordersData[orderCode].activeSessions) ordersData[orderCode].activeSessions = {};
      const cleanKey = workerName.replace(/[.#$\[\]]/g, "_");
      ordersData[orderCode].activeSessions[cleanKey] = { name: workerName, timestamp: Date.now() };
      
      io.emit('sessions-updated', { orderCode, activeSessions: ordersData[orderCode].activeSessions });
    }
  });

  // 3. تحديث أو إضافة طلبية حماية ضد المسح الشامل
  socket.on('update-data', (data) => {
    if (data && typeof data === 'object') {
      // دمج الطلبيات الجديدة بدلاً من مسح القديم
      ordersData = { ...ordersData, ...data };
      io.emit('data-changed', ordersData);
    }
  });

  // حذف طلبية واحدة فقط عند إنهاؤها من الأدمن
  socket.on('delete-order', ({ orderCode }) => {
    if (ordersData[orderCode]) {
      delete ordersData[orderCode];
      io.emit('data-changed', ordersData);
    }
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

app.get('/', (req, res) => {
  res.send('BranchPrep Secured Fast Server is Running Successfully!');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
