const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

// Optional: in-memory data store for active drivers
const drivers = {};

io.on("connection", (socket) => {
  console.log("✅ A client connected:", socket.id);

  // ROLE REGISTRATION
  socket.on("registerRole", (role) => {
    if (role === "user" || role === "driver") {
      socket.role = role;
      socket.join(role);
      console.log(`🆔 ${socket.id} registered as ${role}`);
    } else {
      console.log(`⚠️ Unknown role from ${socket.id}: ${role}`);
    }
  });

  // LOCATION UPDATES
  socket.on("updateLocation", (data) => {
    console.log(`📍 Location from ${socket.role}:`, data);

    if (socket.role === "driver") {
      io.to("user").emit("locationUpdate", {
        ...data,
        from: "driver",
      });
    } else if (socket.role === "user") {
      io.to("driver").emit("userLocation", {
        ...data,
        from: "user",
      });
    }
  });

  // ROUTE UPDATE (driver → users)
  socket.on("routeUpdate", (data) => {
    console.log("🛣️ Route data received from driver:", data);

    // Broadcast route geometry (polyline) to all users
    io.to("user").emit("routeUpdate", {
      ...data,
      from: "driver",
    });
  });

  // 🧍 PASSENGER COUNT UPDATES (driver → users)
  socket.on("passengerUpdate", (data) => {
    const { accountId, passengerCount } = data;
    console.log(`🧍 Passenger count update from driver ${accountId}: ${passengerCount}`);

    // Store latest passenger count per driver
    if (accountId) {
      drivers[accountId] = {
        ...drivers[accountId],
        passengerCount,
        lastUpdated: new Date().toISOString(),
      };
    }

    // Broadcast passenger count to all connected users
    io.to("user").emit("passengerCountUpdate", {
      accountId,
      passengerCount,
      from: "driver",
    });
  });

  // DISCONNECT HANDLER
  socket.on("disconnect", () => {
    console.log(`❌ Client disconnected: ${socket.id} (${socket.role || "unknown"})`);
  });
});

server.listen(3000, () => console.log("✅ Server running on port 3000"));
