const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
app.use(cors({ origin: "*" }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true,
  },
  transports: ["websocket", "polling"], // ✅ Explicitly allow both
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
    console.log(
      `📍 Location from ${socket.role} (${data.accountId} - ${data.organizationName}) → ${data.destinationName} [${data.passengerCount}/${data.maxCapacity}]:`,
      data
    );

    if (socket.role === "driver") {
      if (data.accountId) {
        drivers[data.accountId] = {
          ...drivers[data.accountId],
          lat: data.lat,
          lng: data.lng,
          destinationLat: data.destinationLat,
          destinationLng: data.destinationLng,
          destinationName: data.destinationName || "Unknown",
          organizationName: data.organizationName || "No Organization",
          maxCapacity: data.maxCapacity || 0,
          passengerCount: data.passengerCount || 0,
          profileImageUrl: data.profileImageUrl || "",
          lastUpdated: new Date().toISOString(),
        };
      }

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

  // ROUTE UPDATE
  socket.on("routeUpdate", (data) => {
    console.log(
      `🛣️ Route data received from driver ${data.accountId} → ${data.destinationName}:`,
      data
    );

    if (data.accountId) {
      drivers[data.accountId] = {
        ...drivers[data.accountId],
        geometry: data.geometry,
        destinationLat: data.destinationLat,
        destinationLng: data.destinationLng,
        destinationName: data.destinationName || "Unknown",
        lastUpdated: new Date().toISOString(),
      };
    }

    io.to("user").emit("routeUpdate", {
      ...data,
      from: "driver",
    });
  });

  // PASSENGER UPDATE
  socket.on("passengerUpdate", (data) => {
    const { accountId, passengerCount, maxCapacity, organizationName } = data;
    console.log(
      `🧍 Passenger update from driver ${accountId} (${organizationName}): ${passengerCount}/${maxCapacity}`
    );

    if (accountId) {
      drivers[accountId] = {
        ...drivers[accountId],
        passengerCount,
        maxCapacity,
        organizationName: organizationName || "No Organization",
        lastUpdated: new Date().toISOString(),
      };
    }

    io.to("user").emit("passengerCountUpdate", {
      accountId,
      passengerCount,
      maxCapacity,
      organizationName: organizationName || "No Organization",
      from: "driver",
    });
  });

  // DESTINATION UPDATE
  socket.on("destinationUpdate", (data) => {
    const { accountId, destinationName, destinationLat, destinationLng } = data;
    console.log(
      `🎯 Destination update from driver ${accountId}: ${destinationName} (${destinationLat}, ${destinationLng})`
    );

    if (accountId) {
      drivers[accountId] = {
        ...drivers[accountId],
        destinationName: destinationName || "Unknown",
        destinationLat,
        destinationLng,
        lastUpdated: new Date().toISOString(),
      };
    }

    io.to("user").emit("destinationUpdate", {
      accountId,
      destinationName: destinationName || "Unknown",
      destinationLat,
      destinationLng,
      from: "driver",
    });
  });

  // PROFILE IMAGE UPDATE
  socket.on("profileImageUpdate", (data) => {
    const { accountId, profileImageUrl } = data;
    console.log(`🖼️ Profile image update from driver ${accountId}: ${profileImageUrl}`);

    if (accountId) {
      drivers[accountId] = {
        ...drivers[accountId],
        profileImageUrl: profileImageUrl || "",
        lastUpdated: new Date().toISOString(),
      };
    }

    io.to("user").emit("profileImageUpdate", {
      accountId,
      profileImageUrl: profileImageUrl || "",
      from: "driver",
    });
  });

  // REQUEST DRIVERS DATA
  socket.on("requestDriversData", () => {
    console.log(`📋 User ${socket.id} requested all active drivers data`);
    socket.emit("driversData", {
      drivers: Object.entries(drivers).map(([accountId, data]) => ({
        accountId,
        ...data,
      })),
    });
  });

  // DISCONNECT
  socket.on("disconnect", () => {
    console.log(`❌ Client disconnected: ${socket.id} (${socket.role || "unknown"})`);
  });
});

// ✅ Use Render-provided port (if deployed)
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
