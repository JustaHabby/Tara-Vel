const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

io.on("connection", (socket) => {
  console.log("✅ A client connected:", socket.id);

// PARA SA ROLE NG APPLICATION
  socket.on("registerRole", (role) => {
    if (role === "user" || role === "driver") {
      socket.role = role;
      socket.join(role);
      console.log(`🆔 ${socket.id} registered as ${role}`);
    } else {
      console.log(`⚠️ Unknown role from ${socket.id}: ${role}`);
    }
  });

  // LOCATION UPDATE NIGGAS
  socket.on("updateLocation", (data) => {
    console.log(`📍 Location from ${socket.role}:`, data);

    // DRIVER SEND A LOC SA USERS
    if (socket.role === "driver") {
      io.to("user").emit("locationUpdate", {
        ...data,
        from: "driver",
      });
    }

    // USER SENDS
    else if (socket.role === "user") {
      io.to("driver").emit("userLocation", {
        ...data,
        from: "user",
      });
    }
  });

  // 3️⃣ Handle disconnects
  socket.on("disconnect", () => {
    console.log(`❌ Client disconnected: ${socket.id} (${socket.role})`);
  });
});

server.listen(3000, () => console.log("✅ Server running on port 3000"));
