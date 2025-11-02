const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

// In-memory data store for active drivers
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

  // LOCATION UPDATES (includes accountId, destination, organization, capacity, profile image)
  socket.on("updateLocation", (data) => {
    // data: { lat, lng, destinationLat, destinationLng, destinationName, accountId, organizationName, maxCapacity, passengerCount, profileImageUrl }
    console.log(`📍 Location from ${socket.role} (${data.accountId} - ${data.organizationName}) → ${data.destinationName} [${data.passengerCount}/${data.maxCapacity}]:`, data);

    if (socket.role === "driver") {
      // Update driver location in memory
      if (data.accountId) {
        drivers[data.accountId] = {
          ...drivers[data.accountId],
          lat: data.lat,
          lng: data.lng,
          destinationLat: data.destinationLat,
          destinationLng: data.destinationLng,
          destinationName: data.destinationName || "Unknown", // 🎯 Store destination name
          organizationName: data.organizationName || "No Organization", // 🏢 Store organization
          maxCapacity: data.maxCapacity || 0, // 🚌 Store capacity
          passengerCount: data.passengerCount || 0, // 🧍 Store passenger count
          profileImageUrl: data.profileImageUrl || "", // 🖼️ Store profile image URL
          lastUpdated: new Date().toISOString(),
        };
      }

      // Broadcast to all users
      io.to("user").emit("locationUpdate", {
        ...data,
        from: "driver",
        accountId: data.accountId,
        destinationName: data.destinationName || "Unknown", // 🎯 Broadcast destination name
        organizationName: data.organizationName || "No Organization", // 🏢 Broadcast organization
        maxCapacity: data.maxCapacity || 0, // 🚌 Broadcast capacity
        passengerCount: data.passengerCount || 0, // 🧍 Broadcast passenger count
        profileImageUrl: data.profileImageUrl || "", // 🖼️ Broadcast profile image URL
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
    // data: { accountId, geometry, destinationLat, destinationLng, destinationName }
    console.log(`🛣️ Route data received from driver ${data.accountId} → ${data.destinationName}:`, data);

    // Store route in memory
    if (data.accountId) {
      drivers[data.accountId] = {
        ...drivers[data.accountId],
        geometry: data.geometry,
        destinationLat: data.destinationLat,
        destinationLng: data.destinationLng,
        destinationName: data.destinationName || "Unknown", // 🎯 Store destination name
        lastUpdated: new Date().toISOString(),
      };
    }

    // Broadcast route geometry to all users
    io.to("user").emit("routeUpdate", {
      ...data,
      from: "driver",
      accountId: data.accountId,
    });
  });

  // 🧍 PASSENGER COUNT UPDATES (driver → users)
  socket.on("passengerUpdate", (data) => {
    const { accountId, passengerCount, maxCapacity, organizationName } = data;
    console.log(
      `🧍 Passenger update from driver ${accountId} (${organizationName}): ${passengerCount}/${maxCapacity}`
    );

    // Store latest passenger count, maxCapacity, AND organization info per driver
    if (accountId) {
      drivers[accountId] = {
        ...drivers[accountId],
        passengerCount,
        maxCapacity,
        organizationName: organizationName || "No Organization", // 🏢 Store organization
        lastUpdated: new Date().toISOString(),
      };
    }

    // Broadcast passenger count, maxCapacity, AND organization info to all connected users
    io.to("user").emit("passengerCountUpdate", {
      accountId,
      passengerCount,
      maxCapacity,
      organizationName: organizationName || "No Organization", // 🏢 Broadcast organization
      from: "driver",
    });
  });

  // 🎯 DESTINATION UPDATE (driver → users)
  socket.on("destinationUpdate", (data) => {
    const { accountId, destinationName, destinationLat, destinationLng } = data;
    console.log(
      `🎯 Destination update from driver ${accountId}: ${destinationName} (${destinationLat}, ${destinationLng})`
    );

    // Store destination info per driver
    if (accountId) {
      drivers[accountId] = {
        ...drivers[accountId],
        destinationName: destinationName || "Unknown",
        destinationLat,
        destinationLng,
        lastUpdated: new Date().toISOString(),
      };
    }

    // Broadcast destination info to all connected users
    io.to("user").emit("destinationUpdate", {
      accountId,
      destinationName: destinationName || "Unknown",
      destinationLat,
      destinationLng,
      from: "driver",
    });
  });

  // 🖼️ PROFILE IMAGE UPDATE (driver → users)
  socket.on("profileImageUpdate", (data) => {
    const { accountId, profileImageUrl } = data;
    console.log(
      `🖼️ Profile image update from driver ${accountId}: ${profileImageUrl}`
    );

    // Store profile image URL per driver
    if (accountId) {
      drivers[accountId] = {
        ...drivers[accountId],
        profileImageUrl: profileImageUrl || "",
        lastUpdated: new Date().toISOString(),
      };
    }

    // Broadcast profile image URL to all connected users
    io.to("user").emit("profileImageUpdate", {
      accountId,
      profileImageUrl: profileImageUrl || "",
      from: "driver",
    });
  });

  // 🆕 GET ALL ACTIVE DRIVERS (for new users connecting)
  socket.on("requestDriversData", () => {
    console.log(`📋 User ${socket.id} requested all active drivers data`);
    
    // Send current state of all drivers
    socket.emit("driversData", {
      drivers: Object.entries(drivers).map(([accountId, data]) => ({
        accountId,
        ...data,
      })),
    });
  });

  // DISCONNECT HANDLER
  socket.on("disconnect", () => {
    console.log(
      `❌ Client disconnected: ${socket.id} (${socket.role || "unknown"})`
    );
    
    // Optional: Remove driver from active list after disconnect
    // (You might want to add a timeout instead of immediate removal)
  });
});

server.listen(3000, () => console.log("✅ Server running on port 3000"));
