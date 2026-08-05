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

  socket.emit('init-data', ordersData);

  socket.on('update-data', (data) => {
    ordersData = data;
    io.emit('data-changed', ordersData);
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

app.get('/', (req, res) => {
  res.send('BranchPrep Server is Running Successfully!');
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
