/**
 * ===============================
 * 🚌 Real-Time Bus Tracking Server (Relay + On-Demand Info)
 * ===============================
 * @version 10.0.0
 * @author TaraVel Team
 */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

// ========== CONFIGURATION ==========

const IS_DEV = true;
const STALE_DRIVER_TIMEOUT = 5 * 60 * 1000;
const DISCONNECT_GRACE_PERIOD = 30 * 1000;
const MAX_RECONNECT_ATTEMPTS = 3;
const LOCATION_CHANGE_THRESHOLD = 0.0001;
const LOCATION_UPDATE_INTERVAL = 15000;
const MAX_LOCATION_UPDATES_PER_MINUTE = 10;
const CLEANUP_INTERVAL = 60000;
const MAX_SNAPSHOT_DRIVERS = 50;
const STALE_USER_TIMEOUT = 5 * 60 * 1000;

// ========== SOCKET.IO WITH OPTIMIZATION ==========
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  pingTimeout: 60000, // 60 seconds - how long to wait for pong response
  pingInterval: 25000, // 25 seconds - how often to send ping
  maxHttpBufferSize: 1e6, // 1MB - maximum message size
  compression: true, // Enable compression for large payloads (like route geometry)

  transports: ["websocket", "polling"], // Prefer WebSocket, fallback to polling
});

// ========== IN-MEMORY DATA STORES ==========
const drivers = {};
const socketToAccountId = {};
const rateLimitMap = {};
const users = {};
const accountIdToSocketId = {};
// Session management for duplicate connection prevention
const sessionKeyToSocketId = {}; // Maps sessionKey -> socketId
const socketIdToSessionKey = {}; // Maps socketId -> sessionKey
const sessions = {}; // Maps sessionKey -> { accountId, role, createdAt, lastActivity }

// ========== HELPER FUNCTIONS ==========

/**
 * Calculate the distance between two coordinates using a simplified Euclidean distance formula.
 */
function calculateDistance(lat1, lng1, lat2, lng2) {
  if (!lat1 || !lng1 || !lat2 || !lng2) return Infinity;
  const dLat = Math.abs(lat1 - lat2);
  const dLng = Math.abs(lng1 - lng2);
  return Math.sqrt(dLat * dLat + dLng * dLng);
}

/**
 * Check if a socket has exceeded the rate limit for location updates.
 */
function checkRateLimit(
  socketId,
  maxPerMinute = MAX_LOCATION_UPDATES_PER_MINUTE
) {
  const now = Date.now();
  const limit = rateLimitMap[socketId];

  // If no limit exists or the reset time has passed, create a new limit window
  if (!limit || now > limit.resetTime) {
    rateLimitMap[socketId] = { count: 1, resetTime: now + 60000 };
    return true;
  }

  // Check if limit exceeded
  if (limit.count >= maxPerMinute) {
    return false;
  }

  // Increment counter
  limit.count++;
  return true;
}

/**
 * Clean up stale drivers from memory.
 * Drivers that haven't sent an update in STALE_DRIVER_TIMEOUT milliseconds are considered inactive and are removed from the drivers store.
 */
function cleanupStaleDrivers() {
  const now = Date.now();
  let cleaned = 0;
  for (const [accountId, driver] of Object.entries(drivers)) {
    const timeSinceUpdate = now - new Date(driver.lastUpdated).getTime();
    const timeSinceDisconnect = driver.disconnectedAt
      ? now - driver.disconnectedAt
      : 0;
    const isDisconnected = driver.disconnected === true;
    const gracePeriodExpired =
      isDisconnected && timeSinceDisconnect > DISCONNECT_GRACE_PERIOD;

    let socketExists = false;
    let socketConnected = false;
    
    if (driver.socketId) {
      const socket = io.sockets.sockets.get(driver.socketId);
      socketExists = socket !== undefined;
      socketConnected = socket && socket.connected === true;
    }
    
    if (driver.socketId && (!socketExists || !socketConnected)) {
      if (!isDisconnected) {
        driver.disconnected = true;
        driver.disconnectedAt = Date.now();
        driver.socketId = null;
        delete accountIdToSocketId[accountId];
        if (IS_DEV) {
          log(`🔌 [${accountId}] Driver socket disconnected (detected during cleanup)`);
        }
      }
    }

    if (timeSinceUpdate > STALE_DRIVER_TIMEOUT) {
      if (!isDisconnected || gracePeriodExpired) {
        delete drivers[accountId];
        if (driver.socketId) {
          delete socketToAccountId[driver.socketId];
        }
        delete accountIdToSocketId[accountId];
        cleaned++;

        if (IS_DEV) {
          const reason = isDisconnected
            ? `(disconnected ${Math.round(
                timeSinceDisconnect / 1000
              )}s ago, grace period expired, ${
                driver.reconnectAttempts || 0
              } reconnects)`
            : `(no updates for ${Math.round(timeSinceUpdate / 1000)}s)`;
          log(`🗑️ Cleaned up stale driver ${accountId} ${reason}`);
        }
      } else if (IS_DEV) {
        // Driver is disconnected but still in grace period
        const remainingTime = Math.round(
          (DISCONNECT_GRACE_PERIOD - timeSinceDisconnect) / 1000
        );
        const reconnectCount = driver.reconnectAttempts || 0;
        log(
          `⏳ [${accountId}] Disconnected driver in grace period (${remainingTime}s remaining, ${reconnectCount}/${MAX_RECONNECT_ATTEMPTS} reconnects)`
        );
      }
    } else if (isDisconnected && IS_DEV) {
      const remainingTime = Math.round(
        (DISCONNECT_GRACE_PERIOD - timeSinceDisconnect) / 1000
      );
      if (remainingTime > 0) {
        log(
          `⏳ [${accountId}] Disconnected but has recent updates (${remainingTime}s remaining in grace period)`
        );
      }
    }
  }

  if (cleaned > 0 && IS_DEV) {
    console.log(`🧹 Cleaned up ${cleaned} stale driver(s)`);
  }
}

function cleanupStaleUsers() {
  const now = Date.now();
  let cleaned = 0;
  
  for (const [accountId, user] of Object.entries(users)) {
    const timeSinceActivity = now - user.lastActivity;
    const timeSinceDisconnect = user.disconnectedAt ? (now - user.disconnectedAt) : 0;
    const isDisconnected = user.disconnected === true;
    
    let socketExists = false;
    let socketConnected = false;
    
    if (user.socketId) {
      const socket = io.sockets.sockets.get(user.socketId);
      socketExists = socket !== undefined;
      socketConnected = socket && socket.connected === true;
    }
    
    if (user.socketId && (!socketExists || !socketConnected)) {
      if (!isDisconnected) {
        user.disconnected = true;
        user.disconnectedAt = Date.now();
        user.socketId = null;
        delete accountIdToSocketId[accountId];
        if (IS_DEV) {
          log(`🔌 [${accountId}] User socket disconnected (detected during cleanup)`);
        }
      }
    }
    
    const gracePeriodExpired = isDisconnected && timeSinceDisconnect > DISCONNECT_GRACE_PERIOD;
    
    if (timeSinceActivity > STALE_USER_TIMEOUT) {
      if (!isDisconnected || gracePeriodExpired) {
        delete users[accountId];
        if (user.socketId) {
          delete socketToAccountId[user.socketId];
        }
        delete accountIdToSocketId[accountId];
        cleaned++;
        
        if (IS_DEV) {
          const reason = isDisconnected ? 
            `(disconnected ${Math.round(timeSinceDisconnect/1000)}s ago, grace period expired)` :
            `(no activity for ${Math.round(timeSinceActivity/1000)}s)`;
          log(`🗑️ Cleaned up stale user ${accountId} ${reason}`);
        }
      } else if (IS_DEV) {
        const remainingTime = Math.round((DISCONNECT_GRACE_PERIOD - timeSinceDisconnect) / 1000);
        log(`⏳ [${accountId}] Disconnected user in grace period (${remainingTime}s remaining)`);
      }
    }
  }
  
  if (cleaned > 0 && IS_DEV) {
    console.log(`🧹 Cleaned up ${cleaned} stale user(s)`);
  }
}

/**
 * Conditional logging function that reduces console spam in production.
 */
function log(message, level = "info") {
  if (IS_DEV || level === "error") {
    console.log(message);
  }
}

/**
 * Validate location data received from clients.
 */
function validateLocationData(data) {
  if (!data || typeof data !== "object") return false;
  if (!data.accountId || typeof data.accountId !== "string") return false;
  // Coordinates are required for location updates
  if (data.lat === undefined || data.lng === undefined) return false;
  // Handle string coordinates (convert and validate)
  const lat = typeof data.lat === "string" ? parseFloat(data.lat) : data.lat;
  const lng = typeof data.lng === "string" ? parseFloat(data.lng) : data.lng;
  if (typeof lat !== "number" || isNaN(lat) || lat < -90 || lat > 90)
    return false;
  if (typeof lng !== "number" || isNaN(lng) || lng < -180 || lng > 180)
    return false;
  return true;
}

/**
 * Generate a unique session key
 */
function generateSessionKey(accountId) {
  const timestamp = Date.now();
  const random = Math.random().toString(36).substring(2, 15);
  return accountId ? `${accountId}-${timestamp}-${random}` : `${timestamp}-${random}`;
}

/**
 * Disconnect an old socket connection
 */
function disconnectOldSocket(oldSocketId, accountId, role, reason = "new connection established") {
  if (!oldSocketId) return false;
  
  const oldSocket = io.sockets.sockets.get(oldSocketId);
  if (oldSocket && oldSocket.connected) {
    oldSocket.emit("connectionReplaced", {
      message: `A new connection was established for your account. This connection is being closed. Reason: ${reason}`,
      timestamp: new Date().toISOString()
    });
    
    oldSocket.disconnect(true);
    
    // Clean up session mappings
    const oldSessionKey = socketIdToSessionKey[oldSocketId];
    if (oldSessionKey) {
      delete sessionKeyToSocketId[oldSessionKey];
      delete socketIdToSessionKey[oldSocketId];
      delete sessions[oldSessionKey];
    }
    
    log(`🔌 [${accountId || 'unknown'}] Disconnected old ${role} socket ${oldSocketId} (${reason})`);
    return true;
  }
  return false;
}

// ========== EXPRESS ROUTES ==========
app.get("/", (req, res) => {
  res.json({
    status: "running",
    drivers: Object.keys(drivers).length,
    uptime: process.uptime(),
  });
});
app.get("/health", (req, res) => {
  res.json({ status: "healthy", timestamp: new Date().toISOString() });
});

// ========== SOCKET.IO CONNECTION HANDLER ==========

/**
 * Main Socket.IO connection handler
 */
io.on("connection", (socket) => {
  log(`✅ Client connected: ${socket.id}`);

  /**
  * Cleanup function called when a socket disconnects
  * Removes the driver from memory and cleans up mappings
  */
  const cleanup = () => {
    const accountId = socketToAccountId[socket.id];
    const sessionKey = socketIdToSessionKey[socket.id];
    
    if (accountId && drivers[accountId]) {
      if (drivers[accountId].socketId === socket.id) {
        drivers[accountId].disconnected = true;
        drivers[accountId].disconnectedAt = Date.now();
        drivers[accountId].socketId = null;
        log(
          `🔌 [${accountId}] Driver disconnected (grace period: ${
            DISCONNECT_GRACE_PERIOD / 1000
          }s)`
        );
      }
    }
    
    if (accountId && users[accountId]) {
      if (users[accountId].socketId === socket.id) {
        users[accountId].disconnected = true;
        users[accountId].disconnectedAt = Date.now();
        users[accountId].socketId = null;
        log(
          `🔌 [${accountId}] User disconnected (grace period: ${
            DISCONNECT_GRACE_PERIOD / 1000
          }s)`
        );
      }
    }
    
    if (accountId) {
      if (accountIdToSocketId[accountId] === socket.id) {
        delete accountIdToSocketId[accountId];
      }
    }
    
    // Clean up session mappings
    if (sessionKey) {
      if (sessionKeyToSocketId[sessionKey] === socket.id) {
        delete sessionKeyToSocketId[sessionKey];
      }
      delete sessions[sessionKey];
    }
    delete socketIdToSessionKey[socket.id];
    delete socketToAccountId[socket.id];
    delete rateLimitMap[socket.id];
  };

  /**
   * Error handling wrapper for socket event handlers
   */
  const safeHandler = (eventName, handler) => {
    return (...args) => {
      try {
        handler(...args);
      } catch (error) {
        log(`❌ Error in ${eventName}: ${error.message}`, "error");
        socket.emit("error", { message: "Server error processing request" });
      }
    };
  };

  // --- SESSION RESUMPTION ---
  /**
   * resumeSession Event Handler
   * Allows clients to resume an existing session to prevent duplicate connections
   */
  socket.on(
    "resumeSession",
    safeHandler("resumeSession", (sessionKey) => {
      if (!sessionKey || typeof sessionKey !== "string") {
        log(`⚠️ Invalid resumeSession request from ${socket.id}`);
        socket.emit("error", { message: "Invalid session key" });
        return;
      }

      const existingSession = sessions[sessionKey];
      if (!existingSession) {
        log(`⚠️ Session ${sessionKey} not found, falling back to registerRole`);
        // Session not found, treat as new connection
        socket.emit("error", { message: "Session not found. Please register again." });
        return;
      }

      // Check if there's an old socket with this sessionKey
      const oldSocketId = sessionKeyToSocketId[sessionKey];
      if (oldSocketId && oldSocketId !== socket.id) {
        // Disconnect the old socket with same sessionKey
        disconnectOldSocket(oldSocketId, existingSession.accountId, existingSession.role, "session resumed on new connection");
      }

      // Update session mappings
      sessionKeyToSocketId[sessionKey] = socket.id;
      socketIdToSessionKey[socket.id] = sessionKey;
      
      // Update session activity
      existingSession.lastActivity = Date.now();
      sessions[sessionKey] = existingSession;

      // Restore accountId mapping if available
      if (existingSession.accountId) {
        const oldSocketIdByAccount = accountIdToSocketId[existingSession.accountId];
        if (oldSocketIdByAccount && oldSocketIdByAccount !== socket.id) {
          disconnectOldSocket(oldSocketIdByAccount, existingSession.accountId, existingSession.role, "session resumed");
        }
        accountIdToSocketId[existingSession.accountId] = socket.id;
        socketToAccountId[socket.id] = existingSession.accountId;
      }

      // Set role and join room
      socket.role = existingSession.role;
      socket.join(existingSession.role);

      log(`🔄 [${socket.id}] Session resumed: ${sessionKey} (${existingSession.role}${existingSession.accountId ? `, ${existingSession.accountId}` : ""})`);

      // [NEW] - Send driver state back if driver (restore passenger count, capacity, destination, etc.)
      if (existingSession.role === "driver" && existingSession.accountId) {
        const driverAccountId = existingSession.accountId;
        const existingDriver = drivers[driverAccountId];
        
        if (existingDriver) {
          // Restore driver's socket ID and connection status
          existingDriver.socketId = socket.id;
          existingDriver.disconnected = false;
          existingDriver.disconnectedAt = null;
          
          // Send driver state back to client
          socket.emit("driverStateRestored", {
            accountId: driverAccountId,
            passengerCount: existingDriver.passengerCount ?? 0,
            maxCapacity: existingDriver.maxCapacity ?? 0,
            destinationName: existingDriver.destinationName,
            destinationLat: existingDriver.destinationLat,
            destinationLng: existingDriver.destinationLng,
            organizationName: existingDriver.organizationName,
            lat: existingDriver.lat,
            lng: existingDriver.lng,
            lastUpdated: existingDriver.lastUpdated
          });
          
          log(`🔄 [${driverAccountId}] Driver state restored: ${existingDriver.passengerCount ?? 0}/${existingDriver.maxCapacity ?? 0} passengers, destination: ${existingDriver.destinationName || "Unknown"}`);
        } else {
          log(`⚠️ [${driverAccountId}] Driver not found in memory during session resume`);
        }
      }

      // Send drivers snapshot if user
      if (existingSession.role === "user") {
        const userAccountId = existingSession.accountId;
        if (userAccountId && users[userAccountId]) {
          users[userAccountId].lastActivity = Date.now();
          users[userAccountId].socketId = socket.id;
          users[userAccountId].disconnected = false;
          users[userAccountId].disconnectedAt = null;
        }

        let driversArray = Object.values(drivers)
          .filter(
            (driver) => driver.accountId && (driver.lat || driver.geometry)
          )
          .map((driver) => ({
            accountId: driver.accountId,
            lat: driver.lat,
            lng: driver.lng,
            geometry: driver.geometry,
            destinationName: driver.destinationName,
            destinationLat: driver.destinationLat,
            destinationLng: driver.destinationLng,
            passengerCount: driver.passengerCount ?? 0,
            maxCapacity: driver.maxCapacity ?? 0,
            organizationName: driver.organizationName,
            lastUpdated: driver.lastUpdated,
            isOnline: !driver.disconnected,
          }));

        const totalDrivers = driversArray.length;
        if (MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS) {
          driversArray = driversArray
            .sort(
              (a, b) =>
                new Date(b.lastUpdated || 0) - new Date(a.lastUpdated || 0)
            )
            .slice(0, MAX_SNAPSHOT_DRIVERS)
            .map(({ lastUpdated, ...driver }) => driver);
        } else {
          driversArray = driversArray.map(
            ({ lastUpdated, ...driver }) => driver
          );
        }

        socket.emit("driversSnapshot", {
          drivers: driversArray,
          count: driversArray.length,
          total: totalDrivers,
          limited:
            MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS,
        });
      }
    })
  );

  // --- ROLE REGISTRATION ---
  /**
  * registerRole Event Handler
  * Clients must register their role ("user" or "driver") after connecting.
  */
  socket.on(
    "registerRole",
    safeHandler("registerRole", (data) => {
      let role, accountId;
      
      // Debug: Log raw data to understand structure
      if (IS_DEV) {
        log(`🔍 [DEBUG] registerRole received from ${socket.id}: ${JSON.stringify(data)} (type: ${typeof data})`);
      }
      
      // Handle different data formats
      if (typeof data === "string") {
        role = data;
      } else if (data && typeof data === "object") {
        // Extract role and accountId, handling both direct properties and nested structures
        role = data.role || data["role"];
        accountId = data.accountId || data["accountId"];
        
        // Normalize role to string and trim whitespace
        if (role != null) {
          role = String(role).trim();
        }
        
        // Debug: Log extracted values
        if (IS_DEV) {
          log(`🔍 [DEBUG] Extracted role: "${role}" (type: ${typeof role}), accountId: "${accountId}"`);
        }
      } else {
        log(`⚠️ Invalid registerRole data from ${socket.id}: ${JSON.stringify(data)}`);
        return;
      }

      // Validate role (check for null, undefined, or invalid values)
      if (!role || typeof role !== "string" || (role !== "user" && role !== "driver")) {
        log(`⚠️ Unknown role from ${socket.id}: "${role}" (type: ${typeof role}) | Data: ${JSON.stringify(data)}`);
        return;
      }

      if (role === "user" && !accountId) {
        socket.emit("error", { message: "accountId is required for user registration" });
        return;
      }

      // Generate new session key
      const sessionKey = generateSessionKey(accountId);
      const now = Date.now();

      // Check for old socket with same accountId
      if (accountId) {
        const oldSocketId = accountIdToSocketId[accountId];
        if (oldSocketId && oldSocketId !== socket.id) {
          disconnectOldSocket(oldSocketId, accountId, role, "new registration with same accountId");
        }
        accountIdToSocketId[accountId] = socket.id;
        socketToAccountId[socket.id] = accountId;
      }

      // Check if there's an old session with same sessionKey (shouldn't happen, but safety check)
      const oldSocketIdBySession = sessionKeyToSocketId[sessionKey];
      if (oldSocketIdBySession && oldSocketIdBySession !== socket.id) {
        disconnectOldSocket(oldSocketIdBySession, accountId, role, "session key collision");
      }

      // Create and store session
      sessions[sessionKey] = {
        accountId: accountId || null,
        role: role,
        createdAt: now,
        lastActivity: now
      };
      sessionKeyToSocketId[sessionKey] = socket.id;
      socketIdToSessionKey[socket.id] = sessionKey;

      socket.role = role;
      socket.join(role);

      if (rateLimitMap[socket.id]) {
        delete rateLimitMap[socket.id];
      }

      // Emit sessionAssigned event to client
      socket.emit("sessionAssigned", sessionKey);

      log(`🆔 ${socket.id} registered as ${role}${accountId ? ` (${accountId})` : ""} with session ${sessionKey}`);

      // [NEW] - Send driver state back if driver exists in memory (handles session not found case)
      if (role === "driver" && accountId) {
        const existingDriver = drivers[accountId];
        
        if (existingDriver) {
          // Update driver's socket ID and connection status
          existingDriver.socketId = socket.id;
          existingDriver.disconnected = false;
          existingDriver.disconnectedAt = null;
          
          // Send driver state back to client
          socket.emit("driverStateRestored", {
            accountId: accountId,
            passengerCount: existingDriver.passengerCount ?? 0,
            maxCapacity: existingDriver.maxCapacity ?? 0,
            destinationName: existingDriver.destinationName,
            destinationLat: existingDriver.destinationLat,
            destinationLng: existingDriver.destinationLng,
            organizationName: existingDriver.organizationName,
            lat: existingDriver.lat,
            lng: existingDriver.lng,
            lastUpdated: existingDriver.lastUpdated
          });
          
          log(`🔄 [${accountId}] Driver state restored on registerRole: ${existingDriver.passengerCount ?? 0}/${existingDriver.maxCapacity ?? 0} passengers, destination: ${existingDriver.destinationName || "Unknown"}`);
        }
      }

      if (role === "user") {
        users[accountId] = {
          accountId,
          socketId: socket.id,
          lastActivity: now,
          connectedAt: now,
          disconnected: false,
          disconnectedAt: null
        };
        let driversArray = Object.values(drivers)
          .filter(
            (driver) => driver.accountId && (driver.lat || driver.geometry)
          )
          .map((driver) => ({
            accountId: driver.accountId,
            lat: driver.lat,
            lng: driver.lng,
            geometry: driver.geometry,
            destinationName: driver.destinationName,
            destinationLat: driver.destinationLat,
            destinationLng: driver.destinationLng,
            passengerCount: driver.passengerCount ?? 0,
            maxCapacity: driver.maxCapacity ?? 0,
            organizationName: driver.organizationName,
            lastUpdated: driver.lastUpdated, // server-only for sorting
            isOnline: !driver.disconnected, // Include connection status
          }));

        const totalDrivers = driversArray.length;
        if (MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS) {
          driversArray = driversArray
            .sort(
              (a, b) =>
                new Date(b.lastUpdated || 0) - new Date(a.lastUpdated || 0)
            )
            .slice(0, MAX_SNAPSHOT_DRIVERS)
            .map(({ lastUpdated, ...driver }) => driver);
          log(
            `⚠️ Snapshot limited to ${MAX_SNAPSHOT_DRIVERS} of ${totalDrivers} drivers`
          );
        } else {
          driversArray = driversArray.map(
            ({ lastUpdated, ...driver }) => driver
          );
        }

        socket.emit("driversSnapshot", {
          drivers: driversArray,
          count: driversArray.length,
          total: totalDrivers,
          limited:
            MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS,
        });

        try {
          const lateJoinSnapshot = Object.values(drivers)
            .filter(
              (driver) => driver.accountId && (driver.lat || driver.geometry)
            )
            .map((driver) => ({
              accountId: driver.accountId,
              lat: driver.lat,
              lng: driver.lng,
              geometry: driver.geometry,
              destinationName: driver.destinationName,
              destinationLat: driver.destinationLat,
              destinationLng: driver.destinationLng,
              passengerCount: driver.passengerCount ?? 0,
              maxCapacity: driver.maxCapacity ?? 0,
              organizationName: driver.organizationName,
              isOnline: !driver.disconnected, // Include connection status
            }));

          socket.emit("currentData", {
            buses: lateJoinSnapshot,
          });

          log(
            `📤 Late joiner snapshot sent: ${lateJoinSnapshot.length} active driver(s) to user ${socket.id}`
          );
        } catch (err) {
          log(`❌ Error sending late joiner snapshot to ${socket.id}:`, err);
        }
      }
    })
  );

  // --- LOCATION UPDATES (Driver → Server → Users) ---
  /** updateLocation Event Handler */
  socket.on(
    "updateLocation",
    safeHandler("updateLocation", (data) => {
      // Validate incoming data
      if (!validateLocationData(data)) {
        log(
          `❌ [${data?.accountId || socket.id}] Invalid location data`,
          "error"
        );
        socket.emit("error", { message: "Invalid location data" });
        return;
      }

      // Rate limiting check - prevent abuse
      if (!checkRateLimit(socket.id, MAX_LOCATION_UPDATES_PER_MINUTE)) {
        const accountId =
          data?.accountId || socketToAccountId[socket.id] || "unknown";
        const role = socket.role || "unregistered";
        log(
          `⚠️ Rate limit exceeded for ${socket.id} (${accountId}, ${role}) - Too many updates sent`,
          "error"
        );
        socket.emit("error", {
          message: "Rate limit exceeded. Please slow down location updates.",
        });
        return;
      }

      // Extract and convert coordinates
      const {
        accountId,
        organizationName,
        destinationName,
        destinationLat,
        destinationLng,
        lat: rawLat,
        lng: rawLng,
        passengerCount,
        maxCapacity,
      } = data;

      const lat = typeof rawLat === "string" ? parseFloat(rawLat) : rawLat;
      const lng = typeof rawLng === "string" ? parseFloat(rawLng) : rawLng;

      const prevDriver = drivers[accountId];
      const now = Date.now();

      // Handle reconnection: If driver was disconnected, restore connection
      if (prevDriver && prevDriver.disconnected) {
        // Driver is reconnecting
        const reconnectAttempts = (prevDriver.reconnectAttempts || 0) + 1;
        const timeDisconnected = prevDriver.disconnectedAt
          ? now - prevDriver.disconnectedAt
          : 0;

        // Clear disconnected status
        prevDriver.disconnected = false;
        prevDriver.disconnectedAt = null;
        prevDriver.reconnectAttempts = reconnectAttempts;

        // Handle socket ID change (driver reconnected with new socket)
        if (prevDriver.socketId && prevDriver.socketId !== socket.id) {
          // Clean up old socket mapping
          delete socketToAccountId[prevDriver.socketId];
          log(
            `🔄 [${accountId}] Reconnected (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}) - Socket changed: ${
              prevDriver.socketId
            } → ${socket.id} (was disconnected ${Math.round(
              timeDisconnected / 1000
            )}s)`
          );
        } else {
          log(
            `🔄 [${accountId}] Reconnected (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}) - Data preserved (was disconnected ${Math.round(
              timeDisconnected / 1000
            )}s)`
          );
        }
      } else if (
        prevDriver &&
        prevDriver.socketId &&
        prevDriver.socketId !== socket.id
      ) {
        if (!prevDriver.disconnected) {
          disconnectOldSocket(prevDriver.socketId, accountId, "driver");
        }
        delete socketToAccountId[prevDriver.socketId];
        log(
          `🔄 [${accountId}] Socket ID changed: ${prevDriver.socketId} → ${socket.id}`
        );
      }
      
      accountIdToSocketId[accountId] = socket.id;

      // Check if coordinates changed (for logging movement)
      const timeSinceLastBroadcast = prevDriver?.lastBroadcastTime ? now - prevDriver.lastBroadcastTime : Infinity;

      // Fix: Added missing ! before prevDriver.lastLng to properly check if lastLng is missing
      const locationChanged = !prevDriver || !prevDriver.lastLat || !prevDriver.lastLng || calculateDistance(lat, lng, prevDriver.lastLat, prevDriver.lastLng) > LOCATION_CHANGE_THRESHOLD;
      const passengerDataChanged = passengerCount !== prevDriver?.passengerCount || maxCapacity !== prevDriver?.maxCapacity;
      const isIntervalUpdate = timeSinceLastBroadcast >= LOCATION_UPDATE_INTERVAL;
      const shouldBroadcast = !prevDriver || locationChanged || passengerDataChanged || isIntervalUpdate;

      drivers[accountId] = {
        ...prevDriver,
        accountId,
        organizationName:
          organizationName || prevDriver?.organizationName || "No Organization",
        destinationName:
          destinationName || prevDriver?.destinationName || "Unknown",
        destinationLat: destinationLat ?? prevDriver?.destinationLat,
        destinationLng: destinationLng ?? prevDriver?.destinationLng,
        lat, // Current location (always updated)
        lng, // Current location (always updated)
        passengerCount: passengerCount ?? prevDriver?.passengerCount ?? 0,
        maxCapacity: maxCapacity ?? prevDriver?.maxCapacity ?? 0,
        lastUpdated: new Date().toISOString(),
        socketId: socket.id, // Update socket ID (handles reconnections)
        disconnected: false, // Ensure driver is marked as connected when receiving updates
        disconnectedAt: null, // Clear disconnect timestamp
        reconnectAttempts: prevDriver?.reconnectAttempts || 0, // Preserve reconnect attempts count
        // Only update lastLat/lastLng when we broadcast (these represent last broadcasted location)
        lastLat: shouldBroadcast ? lat : prevDriver?.lastLat ?? lat,
        lastLng: shouldBroadcast ? lng : prevDriver?.lastLng ?? lng,
        lastBroadcastTime: shouldBroadcast
          ? now
          : prevDriver?.lastBroadcastTime,
      };
      socketToAccountId[socket.id] = accountId;
      accountIdToSocketId[accountId] = socket.id;

      // Broadcast to all users if conditions are met
      if (shouldBroadcast) {
        const broadcastData = {
          from: "driver",
          accountId,
          lat,
          lng,
          destinationName: drivers[accountId].destinationName,
          destinationLat: drivers[accountId].destinationLat,
          destinationLng: drivers[accountId].destinationLng,
          passengerCount: drivers[accountId].passengerCount,
          maxCapacity: drivers[accountId].maxCapacity,
          lastUpdated: drivers[accountId].lastUpdated, // Include timestamp for freshness tracking
          isOnline: true, // Driver is online (receiving updates)
        };

        io.to("user").emit("locationUpdate", broadcastData);

        // Simple log: movement status, location, passengers
        if (locationChanged && prevDriver?.lastLat && prevDriver?.lastLng) {
          const distanceFromLast =
            calculateDistance(lat, lng, prevDriver.lastLat, prevDriver.lastLng) * 111000;
          log(
            `🚌 [${accountId}] Moved ${distanceFromLast.toFixed(
              0
            )}m → (${lat?.toFixed(6)}, ${lng?.toFixed(6)}) | Passengers: ${
              drivers[accountId].passengerCount
            }/${drivers[accountId].maxCapacity}`
          );
        } else if (passengerDataChanged) {
          log(
            `🚌 [${accountId}] Location: (${lat?.toFixed(6)}, ${lng?.toFixed(
              6
            )}) | Passengers changed: ${drivers[accountId].passengerCount}/${
              drivers[accountId].maxCapacity
            }`
          );
        } else {
          log(
            `🚌 [${accountId}] Location: (${lat?.toFixed(6)}, ${lng?.toFixed(
              6
            )}) | Passengers: ${drivers[accountId].passengerCount}/${
              drivers[accountId].maxCapacity
            } | Heartbeat`
          );
        }
      }
      // Note: We don't log updates that aren't broadcast (they're stored but not sent yet)
    })
  );

  // --- DESTINATION UPDATE ---
  socket.on(
    "destinationUpdate",
    safeHandler("destinationUpdate", (data) => {
      if (!data?.accountId) {
        socket.emit("error", { message: "Missing accountId" });
        return;
      }

      const { accountId, destinationName, destinationLat, destinationLng } =
        data;
      const prev = drivers[accountId] || {};

      // Handle reconnection if driver was disconnected
      if (prev.disconnected) {
        const reconnectAttempts = (prev.reconnectAttempts || 0) + 1;
        prev.disconnected = false;
        prev.disconnectedAt = null;
        prev.reconnectAttempts = reconnectAttempts;
        if (prev.socketId && prev.socketId !== socket.id) {
          delete socketToAccountId[prev.socketId];
        }
        log(
          `🔄 [${accountId}] Reconnected via destinationUpdate (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`
        );
      } else if (prev.socketId && prev.socketId !== socket.id) {
        if (!prev.disconnected) {
          disconnectOldSocket(prev.socketId, accountId, "driver");
        }
        delete socketToAccountId[prev.socketId];
      }

      // Update driver data with new destination
      drivers[accountId] = {
        ...prev,
        accountId,
        destinationName: destinationName ?? prev.destinationName ?? "Unknown",
        destinationLat: destinationLat ?? prev.destinationLat,
        destinationLng: destinationLng ?? prev.destinationLng,
        lastUpdated: new Date().toISOString(),
        socketId: socket.id,
        disconnected: false,
        disconnectedAt: null,
      };
      socketToAccountId[socket.id] = accountId;
      accountIdToSocketId[accountId] = socket.id;

      // Immediately broadcast destination change to all users
      io.to("user").emit("destinationUpdate", {
        from: "driver",
        accountId,
        destinationName: drivers[accountId].destinationName,
        destinationLat: drivers[accountId].destinationLat,
        destinationLng: drivers[accountId].destinationLng,
        passengerCount: drivers[accountId].passengerCount ?? 0,
        maxCapacity: drivers[accountId].maxCapacity ?? 0,
      });
      log(
        `🎯 [${accountId}] Destination updated: ${drivers[accountId].destinationName}`
      );
    })
  );

  // --- ROUTE UPDATE ---
  socket.on(
    "routeUpdate",
    safeHandler("routeUpdate", (data) => {
      if (!data?.accountId) {
        socket.emit("error", { message: "Missing accountId" });
        return;
      }

      const { accountId, geometry, destinationLat, destinationLng } = data;
      const prev = drivers[accountId] || {};

      // Handle reconnection if driver was disconnected
      if (prev.disconnected) {
        const reconnectAttempts = (prev.reconnectAttempts || 0) + 1;
        prev.disconnected = false;
        prev.disconnectedAt = null;
        prev.reconnectAttempts = reconnectAttempts;
        if (prev.socketId && prev.socketId !== socket.id) {
          delete socketToAccountId[prev.socketId];
        }
        log(
          `🔄 [${accountId}] Reconnected via routeUpdate (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`
        );
      } else if (prev.socketId && prev.socketId !== socket.id) {
        if (!prev.disconnected) {
          disconnectOldSocket(prev.socketId, accountId, "driver");
        }
        delete socketToAccountId[prev.socketId];
      }

      // Check if route data actually changed
      const prevGeometry = prev.geometry;
      const geometryChanged =
        JSON.stringify(geometry) !== JSON.stringify(prevGeometry);
      const destinationLatChanged =
        destinationLat !== undefined && destinationLat !== prev.destinationLat;
      const destinationLngChanged =
        destinationLng !== undefined && destinationLng !== prev.destinationLng;
      const routeChanged =
        geometryChanged || destinationLatChanged || destinationLngChanged;

      // Always update driver data in memory (for getBusInfo requests)
      drivers[accountId] = {
        ...prev,
        accountId,
        geometry,
        destinationLat: destinationLat ?? prev.destinationLat,
        destinationLng: destinationLng ?? prev.destinationLng,
        lastUpdated: new Date().toISOString(),
        socketId: socket.id,
        disconnected: false,
        disconnectedAt: null,
      };
      socketToAccountId[socket.id] = accountId;
      accountIdToSocketId[accountId] = socket.id;

      // Only broadcast and log if route actually changed
      // This prevents spam when app sends frequent updates with same values
      if (routeChanged) {
        // Broadcast route update to all users
        io.to("user").emit("routeUpdate", {
          from: "driver",
          accountId,
          geometry,
          destinationName: drivers[accountId].destinationName,
          destinationLat: drivers[accountId].destinationLat,
          destinationLng: drivers[accountId].destinationLng,
          passengerCount: drivers[accountId].passengerCount ?? 0,
          maxCapacity: drivers[accountId].maxCapacity ?? 0,
        });

        // Log route update
        if (geometryChanged && geometry) {
          log(`🗺️ [${accountId}] Route updated | Polyline changed`);
        } else {
          log(`🗺️ [${accountId}] Route updated`);
        }
      }
      // If route didn't change, we silently update the data store without broadcasting/logging
    })
  );

  // --- PASSENGER COUNT UPDATE ---
  socket.on(
    "passengerUpdate",
    safeHandler("passengerUpdate", (data) => {
      if (!data?.accountId) {
        socket.emit("error", { message: "Missing accountId" });
        return;
      }

      const { accountId, passengerCount, maxCapacity } = data;
      const prev = drivers[accountId] || {};

      // Handle reconnection if driver was disconnected
      if (prev.disconnected) {
        const reconnectAttempts = (prev.reconnectAttempts || 0) + 1;
        prev.disconnected = false;
        prev.disconnectedAt = null;
        prev.reconnectAttempts = reconnectAttempts;
        if (prev.socketId && prev.socketId !== socket.id) {
          delete socketToAccountId[prev.socketId];
        }
        log(
          `🔄 [${accountId}] Reconnected via passengerUpdate (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`
        );
      } else if (prev.socketId && prev.socketId !== socket.id) {
        if (!prev.disconnected) {
          disconnectOldSocket(prev.socketId, accountId, "driver");
        }
        delete socketToAccountId[prev.socketId];
      }

      // Normalize values (use previous values if not provided)
      const newPassengerCount = passengerCount ?? prev.passengerCount ?? 0;
      const newMaxCapacity = maxCapacity ?? prev.maxCapacity ?? 0;
      const prevPassengerCount = prev.passengerCount ?? 0;
      const prevMaxCapacity = prev.maxCapacity ?? 0;

      // Check if values actually changed
      const passengerCountChanged = newPassengerCount !== prevPassengerCount;
      const maxCapacityChanged = newMaxCapacity !== prevMaxCapacity;
      const valuesChanged = passengerCountChanged || maxCapacityChanged;
      // Always update driver data in memory (for getBusInfo requests)
      drivers[accountId] = {
        ...prev,
        accountId,
        passengerCount: newPassengerCount,
        maxCapacity: newMaxCapacity,
        lastUpdated: new Date().toISOString(),
        socketId: socket.id,
        disconnected: false,
        disconnectedAt: null,
      };
      socketToAccountId[socket.id] = accountId;
      accountIdToSocketId[accountId] = socket.id;

      // Only broadcast and log if values actually changed
      // This prevents spam when app sends frequent updates with same values
      if (valuesChanged) {
        // Broadcast passenger count change to all users
        io.to("user").emit("passengerUpdate", {
          from: "driver",
          accountId,
          passengerCount: drivers[accountId].passengerCount,
          maxCapacity: drivers[accountId].maxCapacity,
        });
        log(
          `🧍 [${accountId}] Passenger count updated: ${drivers[accountId].passengerCount}/${drivers[accountId].maxCapacity}`
        );
      }
      // If values didn't change, we silently update the data store without broadcasting/logging
    })
  );

  // --- USER REQUEST: Get Specific Bus Info ---
  socket.on(
    "getBusInfo",
    safeHandler("getBusInfo", (data) => {
      const userAccountId = socketToAccountId[socket.id];
      if (userAccountId && users[userAccountId]) {
        users[userAccountId].lastActivity = Date.now();
      }

      const { accountId } = data || {};
      if (!accountId) {
        socket.emit("busInfoError", { message: "Missing accountId" });
        return;
      }

      const busData = drivers[accountId];
      if (busData) {
        // Send detailed bus information to the requesting user
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
    })
  );

  // --- USER REQUEST: Get All Active Drivers ---
  socket.on(
    "requestDriversData",
    safeHandler("requestDriversData", () => {
      const userAccountId = socketToAccountId[socket.id];
      if (userAccountId && users[userAccountId]) {
        users[userAccountId].lastActivity = Date.now();
      }

      socket.emit("driversData", {
        drivers: Object.entries(drivers)
          .filter(([_, data]) => data.lat && data.lng) // Only drivers with valid location
          .map(([accountId, data]) => ({
            accountId,
            lat: data.lat,

            lng: data.lng,
            destinationLat: data.destinationLat,
            destinationLng: data.destinationLng,
            passengerCount: data.passengerCount ?? 0,
            maxCapacity: data.maxCapacity ?? 0,
          })),
      });
    })
  );

  // --- USER REQUEST: Get Current Data for Late Joiners (Snapshot Refresh) ---
  socket.on(
    "requestCurrentData",
    safeHandler("requestCurrentData", () => {
      const userAccountId = socketToAccountId[socket.id];
      if (userAccountId && users[userAccountId]) {
        users[userAccountId].lastActivity = Date.now();
      }

      // Re-use the optimized snapshot generation logic from registerRole
      let driversArray = Object.values(drivers)
        // Filter for drivers with location OR geometry data
        .filter((driver) => driver.accountId && (driver.lat || driver.geometry))
        .map((driver) => ({
          accountId: driver.accountId,
          lat: driver.lat,
          lng: driver.lng,
          geometry: driver.geometry, // CRITICAL: Includes the polyline
          destinationName: driver.destinationName,
          destinationLat: driver.destinationLat,
          destinationLng: driver.destinationLng,
          passengerCount: driver.passengerCount ?? 0,
          maxCapacity: driver.maxCapacity ?? 0,
          organizationName: driver.organizationName,
          lastUpdated: driver.lastUpdated, // Used for sorting
          isOnline: !driver.disconnected, // Include connection status
        }));

      // Limit snapshot size if configured (optimization for many drivers)
      const totalDrivers = driversArray.length;
      if ( MAX_SNAPSHOT_DRIVERS > 0 && driversArray.length > MAX_SNAPSHOT_DRIVERS) {
        driversArray = driversArray
          .sort(
            (a, b) =>
              new Date(b.lastUpdated || 0) - new Date(a.lastUpdated || 0)
          )
          .slice(0, MAX_SNAPSHOT_DRIVERS)
          .map(({ lastUpdated, ...driver }) => driver); // Remove lastUpdated
        log(
          `⚠️ Snapshot refresh limited to ${MAX_SNAPSHOT_DRIVERS} of ${totalDrivers} drivers`
        );
      } else {
        // Remove lastUpdated before sending to client (server-only field)
        driversArray = driversArray.map(({ lastUpdated, ...driver }) => driver);
      }

      socket.emit("driversSnapshot", {
        drivers: driversArray,
        count: driversArray.length,
        total: totalDrivers,
        limited:
          MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS,
      });

      log(
        `📤 Sent requested snapshot of ${driversArray.length} driver(s) to user ${socket.id}`
      );
    })
  );

  // --- USER REQUEST: Ping Driver ---
  /**
   * pingDriver Event Handler
   * 
   * Users can ping a specific driver to show their location on the driver's map.
   * The ping appears as a dot/marker on the driver's map at the user's location.
   * Only sent to the specific driver (not broadcasted to all drivers).
   */
  socket.on(
    "pingDriver",
    safeHandler("pingDriver", (data) => {
      const userAccountId = socketToAccountId[socket.id];
      if (userAccountId && users[userAccountId]) {
        users[userAccountId].lastActivity = Date.now();
      }

      const { driverAccountId, lat, lng, passengerCount, userAccountId: pingUserAccountId } = data || {};

      // Validate required fields
      if (!driverAccountId) {
        socket.emit("error", { message: "Missing driverAccountId" });
        return;
      }

      if (lat === undefined || lng === undefined || lat === null || lng === null) {
        socket.emit("error", { message: "Missing user location (lat, lng)" });
        return;
      }

      // Validate passenger count (default to 1 if not provided)
      const validPassengerCount = passengerCount !== undefined && passengerCount !== null 
        ? Math.max(1, Math.floor(Number(passengerCount))) 
        : 1;

      // Check if driver exists and is online
      const driver = drivers[driverAccountId];
      if (!driver) {
        socket.emit("error", { message: "Driver not found" });
        return;
      }

      if (driver.disconnected || !driver.socketId) {
        socket.emit("error", { message: "Driver is offline" });
        return;
      }

      // Get driver's socket ID
      const driverSocketId = accountIdToSocketId[driverAccountId] || driver.socketId;
      if (!driverSocketId) {
        socket.emit("error", { message: "Driver socket not found" });
        return;
      }

      // Send ping ONLY to the specific driver (not broadcasted)
      io.to(driverSocketId).emit("pingReceived", {
        from: "user",
        userAccountId: pingUserAccountId || userAccountId || "unknown",
        lat: lat,
        lng: lng,
        passengerCount: validPassengerCount,
        timestamp: Date.now(),
      });

      log(
        `📍 User ${userAccountId || socket.id} pinged driver ${driverAccountId} at (${lat}, ${lng}) with ${validPassengerCount} passenger(s)`
      );
    })
  );

  // --- DISCONNECT HANDLER ---
  socket.on("disconnect", () => {
    log(`❌ Disconnected: ${socket.id} (${socket.role || "unknown"})`);
    cleanup();
  });
  socket.on("error", (error) => {
    const errorMessage = error?.message || error?.toString() || String(error) || "Unknown error";
    log(`❌ Socket error for ${socket.id}: ${errorMessage}`, "error");
    if (error && typeof error === "object") {
      log(`   Error details: ${JSON.stringify(error)}`, "error");
    }
    cleanup();
  });
});

// ========== PERIODIC CLEANUP TASKS ==========
setInterval(() => {
  cleanupStaleDrivers();
  cleanupStaleUsers();
}, CLEANUP_INTERVAL);
setInterval(() => {
  const now = Date.now();
  for (const [socketId, limit] of Object.entries(rateLimitMap)) {
    if (now > limit.resetTime) {
      delete rateLimitMap[socketId];
    }
  }
}, 60000);
// Every minute

// ========== SERVER START ==========

const PORT = 3000;
const HOST = "0.0.0.0";

server.listen(PORT, HOST, () => {
  console.log(`✅ Server running on ${HOST}:${PORT}`);
  console.log(`📊 Environment: ${IS_DEV ? "Development" : "Production"}`);
  console.log(`⚙️  Compression: Enabled`);
  console.log(`🧹 Cleanup interval: ${CLEANUP_INTERVAL / 1000}s`);
  console.log(
    `📍 Location update interval: ${
      LOCATION_UPDATE_INTERVAL / 1000
    }s (15-second heartbeat enabled)`
  );
});
