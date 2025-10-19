const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

io.on("connection", (socket) => {
  console.log("✅ A client connected:", socket.id);

  socket.on("updateLocation", (data) => {
    console.log("📍 Location received:", data);
    socket.broadcast.emit("locationUpdate", data);
  });

  socket.on("disconnect", () => {
    console.log("❌ A client disconnected:", socket.id);
  });
});

server.listen(3000, () => console.log("✅ Server running on port 3000"));