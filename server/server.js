const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { MongoClient } = require('mongodb');

const app = express();
app.use(cors());
app.use(express.json());
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

/* ============================================================
   BranchPrep Server v2
   - نفس منطق الشغل القديم بالظبط (الأدمن والموظف مش محتاجين أي تعديل)
   - إضافة (1): حفظ دائم في MongoDB — البيانات بترجع بعد أي restart
   - إضافة (2): تطبيق القفل (lock) على السيرفر نفسه لمنع العدّ المزدوج
   ============================================================ */

// ---------- الذاكرة السريعة (نفس فكرة القديم — الشغل اللحظي بيتم من هنا) ----------
let ordersData = {};

// ---------- إعدادات قاعدة البيانات ----------
const MONGODB_URI = process.env.MONGODB_URI || "";
const DB_NAME = process.env.MONGO_DB || "branchprep";
const COLLECTION = "orders";
// مدة بقاء الطلبية قبل ما تتمسح تلقائيًا (بيانات مؤقتة) — افتراضي 3 أيام من آخر تعديل
const ORDER_TTL_DAYS = Number(process.env.ORDER_TTL_DAYS || 3);
const SAVE_INTERVAL_MS = 20000; // بنجمّع التعديلات ونحفظها كل ~20 ثانية بدل كل ضغطة

let mongoClient = null;
let ordersCol = null;
let dbReady = false;
const dirtyOrders = new Set(); // أكواد الطلبيات اللي اتعدّلت ومحتاجة حفظ

function markDirty(orderCode) {
  if (orderCode) dirtyOrders.add(String(orderCode));
}

// حفظ الطلبيات المعدّلة (batch) — بيتنده دوريًا وعند الإغلاق
async function flushDirty() {
  if (!dbReady || dirtyOrders.size === 0) return;
  const codes = Array.from(dirtyOrders);
  dirtyOrders.clear();
  const expireAt = new Date(Date.now() + ORDER_TTL_DAYS * 24 * 60 * 60 * 1000);
  try {
    const ops = [];
    for (const code of codes) {
      if (ordersData[code]) {
        ops.push({
          replaceOne: {
            filter: { _id: code },
            replacement: { _id: code, data: ordersData[code], expireAt },
            upsert: true
          }
        });
      } else {
        // اتمسحت من الذاكرة => نمسحها من القاعدة
        ops.push({ deleteOne: { filter: { _id: code } } });
      }
    }
    if (ops.length) await ordersCol.bulkWrite(ops, { ordered: false });
  } catch (err) {
    console.error('[DB] flush error:', err.message);
    // رجّع الأكواد تاني للمحاولة في الدورة الجاية بدل ما نفقدها
    codes.forEach(c => dirtyOrders.add(c));
  }
}

async function connectDB() {
  if (!MONGODB_URI) {
    console.warn('⚠️  MONGODB_URI غير مضبوط — السيرفر هيشتغل بالذاكرة فقط (من غير حفظ دائم).');
    return;
  }
  try {
    mongoClient = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 8000 });
    await mongoClient.connect();
    const db = mongoClient.db(DB_NAME);
    ordersCol = db.collection(COLLECTION);
    // فهرس TTL: يمسح الطلبية تلقائيًا عند تعدّي expireAt (بيانات مؤقتة)
    try { await ordersCol.createIndex({ expireAt: 1 }, { expireAfterSeconds: 0 }); } catch (e) {}
    // تحميل كل الطلبيات المخزّنة للذاكرة عند الإقلاع
    const docs = await ordersCol.find({}).toArray();
    ordersData = {};
    docs.forEach(d => { if (d && d._id && d.data) ordersData[d._id] = d.data; });
    dbReady = true;
    console.log(`✅ [DB] متصل. تم تحميل ${docs.length} طلبية من قاعدة البيانات.`);
  } catch (err) {
    console.error('❌ [DB] فشل الاتصال — السيرفر هيكمّل بالذاكرة فقط:', err.message);
    dbReady = false;
  }
}

// دورة الحفظ
setInterval(() => { flushDirty(); }, SAVE_INTERVAL_MS);

// ---------- منطق السوكيت (نفس القديم + الإضافتين) ----------
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  socket.emit('init-data', ordersData);

  // 1) التحقق من صحة كود الطلبية
  socket.on('validate-order', ({ orderCode }, callback) => {
    const isValid = !!(ordersData && ordersData[orderCode]);
    if (typeof callback === 'function') callback({ valid: isValid });
  });

  // 2) حجز/قفل صنف
  socket.on('acquire-lock', ({ orderCode, barcode, workerName, sessionId }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      const now = Date.now();
      if (!item.lock || item.lock.sessionId === sessionId || (now - (item.lock.timestamp || 0) > 15000)) {
        item.lock = { worker: workerName, sessionId, timestamp: now };
        markDirty(orderCode);
        io.emit('lock-changed', { orderCode, barcode, lock: item.lock });
      } else {
        socket.emit('lock-changed', { orderCode, barcode, lock: item.lock });
      }
    }
  });

  socket.on('keep-lock-alive', ({ orderCode, barcode, sessionId }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      if (item.lock && item.lock.sessionId === sessionId) {
        item.lock.timestamp = Date.now();
        io.emit('lock-changed', { orderCode, barcode, lock: item.lock });
      }
    }
  });

  socket.on('release-lock', ({ orderCode, barcode, sessionId }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];
      if (item.lock && item.lock.sessionId === sessionId) {
        delete item.lock;
        markDirty(orderCode);
        io.emit('lock-changed', { orderCode, barcode, lock: null });
      }
    }
  });

  // 3) تحديث كمية فرع — مع تطبيق القفل على السيرفر (الإضافة 2)
  socket.on('update-item', ({ orderCode, barcode, branch, actual, worker }) => {
    if (ordersData[orderCode] && ordersData[orderCode].items && ordersData[orderCode].items[barcode]) {
      const item = ordersData[orderCode].items[barcode];

      // ===== حماية القفل على مستوى السيرفر =====
      // لو الصنف مقفول لموظف تاني والقفل لسه حي (< 15 ثانية) => نتجاهل التعديل
      if (item.lock && item.lock.worker && item.lock.worker !== worker) {
        const lockFresh = (Date.now() - (item.lock.timestamp || 0)) < 15000;
        if (lockFresh) {
          // نرجّع للموظف اللي حاول القيمة الحقيقية عشان شاشته تتصحّح لوحدها
          const cur = (item.branches && item.branches[branch]) ? item.branches[branch] : { actual: "", worker: "" };
          socket.emit('item-updated', { orderCode, barcode, branch, actual: cur.actual, worker: cur.worker });
          return;
        }
      }
      // =========================================

      if (!item.branches) item.branches = {};
      if (!item.branches[branch]) item.branches[branch] = { target: 0, actual: "", worker: "" };
      item.branches[branch].actual = actual;
      item.branches[branch].worker = worker;
      ordersData[orderCode]._lastUpdated = Date.now();
      markDirty(orderCode);
      io.emit('item-updated', { orderCode, barcode, branch, actual, worker });
    }
  });

  // نبض تواجد الموظف
  socket.on('worker-heartbeat', ({ orderCode, workerName }) => {
    if (ordersData[orderCode]) {
      if (!ordersData[orderCode].activeSessions) ordersData[orderCode].activeSessions = {};
      const cleanKey = String(workerName).replace(/[.#$\[\]]/g, "_");
      ordersData[orderCode].activeSessions[cleanKey] = { name: workerName, timestamp: Date.now() };
      io.emit('sessions-updated', { orderCode, activeSessions: ordersData[orderCode].activeSessions });
    }
  });

  // إضافة/دمج طلبية (رفع من الأدمن)
  socket.on('update-data', (data) => {
    if (data && typeof data === 'object') {
      ordersData = { ...ordersData, ...data };
      Object.keys(data).forEach(code => markDirty(code));
      flushDirty(); // حفظ فوري للطلبية الجديدة عشان تتسجّل على طول
      io.emit('data-changed', ordersData);
    }
  });

  // حذف طلبية
  socket.on('delete-order', ({ orderCode }) => {
    if (ordersData[orderCode]) {
      delete ordersData[orderCode];
      markDirty(orderCode); // flush هيمسحها من القاعدة لأنها مش موجودة في الذاكرة
      flushDirty();
      io.emit('data-changed', ordersData);
    }
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

app.get('/', (req, res) => {
  res.send('BranchPrep Secured Fast Server v2 (Persistent) is Running Successfully!');
});
app.get('/health', (req, res) => {
  res.json({ ok: true, dbReady, orders: Object.keys(ordersData).length });
});

// ---------- إغلاق آمن: نحفظ أي تعديلات معلّقة قبل ما Render يعمل restart ----------
async function gracefulShutdown() {
  console.log('⏳ إغلاق — جاري حفظ آخر التعديلات...');
  try { await flushDirty(); } catch (e) {}
  try { if (mongoClient) await mongoClient.close(); } catch (e) {}
  process.exit(0);
}
process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

const PORT = process.env.PORT || 3000;
connectDB().finally(() => {
  server.listen(PORT, () => console.log(`Server is running on port ${PORT}`));
});
