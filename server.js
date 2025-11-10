// ===============================
// 🚌 Real-Time Bus Tracking Server (Relay + On-Demand Info)
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

      // 🧠 If user connects, instantly replay all current driver data
      if (role === "user") {
        Object.values(drivers).forEach((driver) => {
          // Send route geometry if exists
          if (driver.geometry) {
            socket.emit("routeUpdate", {
              from: "driver",
              accountId: driver.accountId,
              geometry: driver.geometry,
              destinationLat: driver.destinationLat,
              destinationLng: driver.destinationLng,
            });
          }

          // Send last known location if available
          if (driver.lat && driver.lng) {
            socket.emit("locationUpdate", {
              from: "driver",
              accountId: driver.accountId,
              lat: driver.lat,
              lng: driver.lng,
              destinationLat: driver.destinationLat,
              destinationLng: driver.destinationLng,
            });
          }
        });

        console.log(`📤 Replayed ${Object.keys(drivers).length} active driver(s) to new user ${socket.id}`);
      }
    } else {
      console.log(`⚠️ Unknown role from ${socket.id}: ${role}`);
    }
  });

  // --- LOCATION UPDATES (Driver → Server → Users) ---
  socket.on("updateLocation", (data) => {
    if (!data?.accountId) return;
    const {
      accountId,
      organizationName,
      destinationName,
      destinationLat,
      destinationLng,
      lat,
      lng,
      passengerCount,
      maxCapacity,
    } = data;

    // 🧠 Update memory cache
    drivers[accountId] = {
      ...drivers[accountId],
      accountId,
      organizationName: organizationName || drivers[accountId]?.organizationName || "No Organization",
      destinationName: destinationName || drivers[accountId]?.destinationName || "Unknown",
      destinationLat: destinationLat ?? drivers[accountId]?.destinationLat,
      destinationLng: destinationLng ?? drivers[accountId]?.destinationLng,
      lat,
      lng,
      passengerCount: passengerCount ?? drivers[accountId]?.passengerCount ?? 0,
      maxCapacity: maxCapacity ?? drivers[accountId]?.maxCapacity ?? 0,
      lastUpdated: new Date().toISOString(),
    };

    // 🔁 Broadcast to all users
    io.to("user").emit("locationUpdate", {
      from: "driver",
      accountId,
      lat,
      lng,
      destinationLat,
      destinationLng,
    });

    console.log(
      `📡 [${accountId}] Location broadcast → Bus (${lat}, ${lng}) | Destination (${destinationLat}, ${destinationLng})`
    );
  });

  // --- DESTINATION UPDATE (if sent separately) ---
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

    // Broadcast to users
    io.to("user").emit("destinationUpdate", {
      from: "driver",
      accountId,
      destinationLat,
      destinationLng,
    });

    console.log(`🎯 [${accountId}] Destination updated: (${destinationLat}, ${destinationLng})`);
  });

  // --- ROUTE UPDATE (Driver → Server → Users) ---
  socket.on("routeUpdate", (data) => {
    if (!data?.accountId) return;

    const { accountId, geometry, destinationLat, destinationLng } = data;

    // 🧠 Store route info in memory
    drivers[accountId] = {
      ...drivers[accountId],
      accountId,
      geometry, // encoded polyline string
      destinationLat,
      destinationLng,
      lastUpdated: new Date().toISOString(),
    };

    // 🔁 Broadcast to all users
    io.to("user").emit("routeUpdate", {
      from: "driver",
      accountId,
      geometry,
      destinationLat,
      destinationLng,
    });

    console.log(
      `🗺️ [${accountId}] Sent routeUpdate with encoded polyline + destination (${destinationLat}, ${destinationLng})`
    );
  });

  // --- PASSENGER COUNT UPDATE ---
  socket.on("passengerUpdate", (data) => {
    const { accountId, passengerCount, maxCapacity } = data || {};
    if (!accountId) return;

    const prev = drivers[accountId] || {};
    drivers[accountId] = {
      ...prev,
      passengerCount: passengerCount ?? prev.passengerCount ?? 0,
      maxCapacity: maxCapacity ?? prev.maxCapacity ?? 0,
      lastUpdated: new Date().toISOString(),
    };

    console.log(
      `🧍 Passenger count updated for ${accountId}: ${drivers[accountId].passengerCount}/${drivers[accountId].maxCapacity}`
    );
  });

  // --- USER REQUEST: Get Specific Bus Info ---
  socket.on("getBusInfo", (data) => {
    const { accountId } = data || {};
    if (!accountId)
      return socket.emit("busInfoError", { message: "Missing accountId" });

    const busData = drivers[accountId];
    if (busData) {
      console.log(`ℹ️ Bus info requested for ${accountId}`);
      socket.emit("busInfo", {
        from: "server",
        accountId: busData.accountId,
        organizationName: busData.organizationName,
        destinationName: busData.destinationName,
        destinationLat: busData.destinationLat,
        destinationLng: busData.destinationLng,
        passengerCount: busData.passengerCount,
        maxCapacity: busData.maxCapacity,
        lastUpdated: busData.lastUpdated,
      });
    } else {
      socket.emit("busInfoError", { message: "Bus not found or inactive" });
    }
  });

  // --- USER REQUEST: Get All Active Drivers ---
  socket.on("requestDriversData", () => {
    console.log(`📋 ${socket.id} requested active drivers`);
    socket.emit("driversData", {
      drivers: Object.entries(drivers).map(([accountId, data]) => ({
        accountId,
        lat: data.lat,
        lng: data.lng,
        destinationLat: data.destinationLat,
        destinationLng: data.destinationLng,
      })),
    });
  });

  // --- DISCONNECT HANDLER ---
  socket.on("disconnect", () => {
    console.log(`❌ Disconnected: ${socket.id} (${socket.role || "unknown"})`);

    // 🧹 Optional: remove driver if disconnected
    if (socket.role === "driver") {
      for (const [id, d] of Object.entries(drivers)) {
        if (d.socketId === socket.id) delete drivers[id];
      }
    }
  });
});

// ========== SERVER START ==========
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
