// ===============================
// 🚌 Real-Time Bus Tracking Server
// ===============================
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }, // Allow all origins for dev/testing
});

// In-memory store for active drivers
const drivers = {};

// Root route (for quick check if server is alive)
app.get("/", (req, res) => {
  res.send("🟢 TaraVel Realtime Server is running!");
});

// ========== SOCKET.IO HANDLERS ==========
io.on("connection", (socket) => {
  console.log("✅ A client connected:", socket.id);

  // --- ROLE REGISTRATION ---
  socket.on("registerRole", (role) => {
    if (role === "user" || role === "driver") {
      socket.role = role;
      socket.join(role);
      console.log(`🆔 ${socket.id} registered as ${role}`);
    } else {
      console.log(`⚠️ Unknown role from ${socket.id}: ${role}`);
    }
  });

  // --- LOCATION UPDATES ---
  socket.on("updateLocation", (data) => {
    // Expect: { lat, lng, destinationLat, destinationLng, destinationName, accountId, organizationName }
    if (!data?.accountId) return;

    const filtered = {
      accountId: data.accountId,
      organizationName: data.organizationName || "No Organization",
      destinationName: data.destinationName || "Unknown",
      destinationLat: data.destinationLat,
      destinationLng: data.destinationLng,
      lat: data.lat,
      lng: data.lng,
    };

    if (socket.role === "driver") {
      // Save to memory
      drivers[data.accountId] = {
        ...drivers[data.accountId],
        ...filtered,
        lastUpdated: new Date().toISOString(),
      };

      // Broadcast to users
      io.to("user").emit("locationUpdate", {
        from: "driver",
        ...filtered,
      });
    } else if (socket.role === "user") {
      io.to("driver").emit("userLocation", {
        from: "user",
        ...filtered,
      });
    }
  });

  // --- ROUTE UPDATE (driver → users) ---
  socket.on("routeUpdate", (data) => {
    if (!data?.accountId) return;
    console.log(`🛣️ Route data received from driver ${data.accountId}`);

    drivers[data.accountId] = {
      ...drivers[data.accountId],
      geometry: data.geometry,
      destinationLat: data.destinationLat,
      destinationLng: data.destinationLng,
      destinationName: data.destinationName || "Unknown",
      lastUpdated: new Date().toISOString(),
    };

    io.to("user").emit("routeUpdate", {
      from: "driver",
      ...data,
    });
  });

  // --- PASSENGER COUNT UPDATES ---
  socket.on("passengerUpdate", (data) => {
    const { accountId, passengerCount, maxCapacity, organizationName } = data || {};
    if (!accountId) return;

    const prev = drivers[accountId] || {};
    drivers[accountId] = {
      ...prev,
      passengerCount: passengerCount ?? prev.passengerCount ?? 0,
      maxCapacity: maxCapacity ?? prev.maxCapacity ?? 0,
      organizationName: organizationName || prev.organizationName || "No Organization",
      lastUpdated: new Date().toISOString(),
    };

    io.to("user").emit("passengerCountUpdate", {
      from: "driver",
      accountId,
      passengerCount: drivers[accountId].passengerCount,
      maxCapacity: drivers[accountId].maxCapacity,
      organizationName: drivers[accountId].organizationName,
    });
  });

  // --- DESTINATION UPDATE ---
  socket.on("destinationUpdate", (data) => {
    const { accountId, destinationName, destinationLat, destinationLng } = data || {};
    if (!accountId) return;

    drivers[accountId] = {
      ...drivers[accountId],
      destinationName: destinationName || "Unknown",
      destinationLat,
      destinationLng,
      lastUpdated: new Date().toISOString(),
    };

    io.to("user").emit("destinationUpdate", {
      from: "driver",
      accountId,
      destinationName,
      destinationLat,
      destinationLng,
    });
  });

  // --- NEW FEATURE: GET BUS INFO (user → server → user) ---
  socket.on("getBusInfo", (data) => {
    const { accountId } = data || {};
    if (!accountId) return socket.emit("busInfoError", { message: "Missing accountId" });

    const busData = drivers[accountId];
    if (busData) {
      console.log(`ℹ️ Bus info requested for ${accountId}`);
      socket.emit("busInfo", {
        accountId,
        ...busData,
        from: "server",
      });
    } else {
      socket.emit("busInfoError", { message: "Bus not found or inactive" });
    }
  });

  // --- NEW USERS REQUEST ALL ACTIVE DRIVERS ---
  socket.on("requestDriversData", () => {
    console.log(`📋 ${socket.id} requested active drivers`);
    socket.emit("driversData", {
      drivers: Object.entries(drivers).map(([accountId, data]) => ({
        accountId,
        ...data,
      })),
    });
  });

  // --- DISCONNECT HANDLER ---
  socket.on("disconnect", () => {
    console.log(`❌ Disconnected: ${socket.id} (${socket.role || "unknown"})`);
  });
});

// Use PORT from hosting service or 3000 locally
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
