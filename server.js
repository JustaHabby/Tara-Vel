/**
 * ===============================
 * 🚌 Real-Time Bus Tracking Server (Relay + On-Demand Info)
 * ===============================
 * * This server handles real-time location updates from bus drivers and relays them to users.
 * It's designed to work with apps that send location updates every 15 seconds.
 * * Architecture:
 * - Drivers send location updates via WebSocket
 * - Server stores driver data in memory
 * - Server broadcasts updates to all connected users
 * - Optimized to handle 15-second update intervals efficiently
 * * Key Features:
 * - 15-second interval heartbeat: Always broadcasts at 15-second intervals (even if bus hasn't moved)
 * - Smart broadcasting: Only broadcasts significant location changes to reduce network traffic
 * - Rate limiting: Prevents abuse by limiting updates per minute
 * - Memory management: Automatically cleans up stale drivers
 * - Batch snapshot: New users receive all driver 
 data in a single message
 * * @author TaraVel Team
 */

 const express = require("express");
 const http = require("http");
 const { Server } = require("socket.io");
 
 const app = express();
 const server = http.createServer(app);
 
 // ========== CONFIGURATION ==========
 
 /** Enable development logging (set to false in production to reduce console output) */
 const IS_DEV = true;
/** * Time in milliseconds before a driver is considered stale and removed from memory.
 * Drivers that haven't sent updates in this time will be cleaned up.
 * Default: 5 minutes (300,000ms)
 */
const STALE_DRIVER_TIMEOUT = 5 * 60 * 1000;
/**
 * Grace period after disconnect before driver data is removed (in milliseconds).
 * During this period, driver data is preserved to allow for reconnection.
 * If driver reconnects within this period, data is restored.
 * Default: 30 seconds (30,000ms)
 */
const DISCONNECT_GRACE_PERIOD = 30 * 1000;
/**
 * Maximum number of reconnection attempts allowed within grace period.
 * After this many reconnections, driver data will be kept but marked as potentially unstable.
 * Default: 3 reconnection attempts
 */
const MAX_RECONNECT_ATTEMPTS = 3;
 /**
  * Minimum distance change (in degrees) required to trigger a location broadcast.
  * This prevents broadcasting tiny GPS fluctuations while the bus is stationary.
  * ~0.0001 degrees ≈ ~11 meters at equator
  * * If a bus moves less than this distance, the update is stored but not broadcast
  * (unless 15 seconds have passed - see LOCATION_UPDATE_INTERVAL)
  */
 const LOCATION_CHANGE_THRESHOLD = 0.0001;
 /**
  * Expected update interval from the mobile app (in milliseconds).
  * The app sends location updates every 15 seconds.
  * * IMPORTANT: The server will ALWAYS broadcast location updates if 15+ seconds have passed
  * since the last broadcast, even if the bus hasn't moved.
  This ensures:
  * 1. Users see regular updates (heartbeat) even when bus is stopped
  * 2. The app's 15-second update cycle is respected
  * 3. Real-time tracking remains consistent
  * * Default: 15,000ms (15 seconds)
  */
 const LOCATION_UPDATE_INTERVAL = 15000;
 /**
  * Maximum number of location updates allowed per minute per socket.
  * This prevents abuse and spam attacks.
  * * For 15-second intervals: 60 seconds / 15 seconds = 4 updates per minute
  * Setting to 10 provides a 2.5x buffer for network delays and retries.
  * * Default: 10 updates per minute
  */
 const MAX_LOCATION_UPDATES_PER_MINUTE = 10;
 /**
  * Interval for cleaning up stale drivers (in milliseconds).
  * Runs every minute to remove drivers that haven't sent updates.
  * * Default: 60,000ms (1 minute)
  */
 const CLEANUP_INTERVAL = 60000;
 /**
  * Maximum number of drivers to include in initial snapshot for new users.
  * This prevents huge payloads when there are many active drivers.
  * Set to 0 or null to send all drivers.
  * * Default: 50 drivers
  */
 const MAX_SNAPSHOT_DRIVERS = 50;
 
 // ========== SOCKET.IO WITH OPTIMIZATION ==========
 
 /**
  * Socket.IO server configuration
  * Optimized for version
  */
 const io = new Server(server, {
   cors: { 
     origin: "*",
     methods: ["GET", "POST"]
   },
   pingTimeout: 60000,      // 60 seconds - how long to wait for pong response
   pingInterval: 25000,     // 25 seconds - how often to send ping
   maxHttpBufferSize: 1e6,  // 1MB - maximum message size
   compression: true,       // Enable compression for large payloads (like route geometry)
  
   transports: ["websocket", "polling"], // Prefer WebSocket, fallback to polling
 });
 
 // ========== IN-MEMORY DATA STORES ==========
 
/**
 * Active drivers data store
 * Structure: { accountId: { 
 * accountId: string,
 * socketId: string | null (null if disconnected),
 * lat: number,
 * lng: number,
 * destinationLat: number,
 * destinationLng: number,
 * destinationName: string,
 * organizationName: string,
 * geometry: object (route geometry data),
 * passengerCount: number,
 * maxCapacity: number,
 * lastUpdated: string (ISO timestamp),
 * lastLat: number (last broadcasted latitude),
 * lastLng: number (last broadcasted longitude),
 * lastBroadcastTime: number (timestamp of last broadcast),
 * disconnected: boolean (true if driver disconnected but in grace period),
 * disconnectedAt: number | null (timestamp when disconnected, null if connected),
 * reconnectAttempts: number (number of times driver reconnected within grace period)
 * } }
 */
const drivers = {};
 /**
  * Mapping from socket.id to accountId for quick lookup during cleanup
  * Structure: { socketId: accountId }
  */
 const socketToAccountId = {};
 /**
  * Rate limiting tracker
  * Structure: { socketId: { count: number, resetTime: number } }
  * Tracks how many updates each socket has sent in the current minute
  */
 const rateLimitMap = {};
 
 // ========== HELPER FUNCTIONS ==========
 
 /**
  * Calculate the distance between two coordinates using a simplified Euclidean distance formula.
  * * NOTE: This is a simplified formula that works well for small distances (within a city).
  * For more accurate results over larger distances, use the Haversine formula.
  * * @param {number} lat1 - Latitude of first point
  * @param {number} lng1 - Longitude of first point
  * @param {number} lat2 - Latitude of second point
  * @param {number} lng2 - Longitude of second point
  * @returns {number} Distance in degrees (approximately ~111km per degree at equator)
  * Returns Infinity if any coordinate is missing
  * * @example
  * // Check if bus moved more than threshold
  * const distance = calculateDistance(prevLat, prevLng, newLat, newLng);
  * if (distance > LOCATION_CHANGE_THRESHOLD) {
  * // Bus moved significantly, broadcast update
  * }
  */
 function calculateDistance(lat1, lng1, lat2, lng2) {
   if (!lat1 || !lng1 || !lat2 || !lng2) return Infinity;
   const dLat = Math.abs(lat1 - lat2);
   const dLng = Math.abs(lng1 - lng2);
   return Math.sqrt(dLat * dLat + dLng * dLng);
 }
 
 /**
  * Check if a socket has exceeded the rate limit for location updates.
  * * This function implements a sliding window rate limiter that allows
  * MAX_LOCATION_UPDATES_PER_MINUTE updates per 60-second window.
  * * @param {string} socketId - The socket ID to check
  * @param {number} maxPerMinute - Maximum updates allowed per minute (default: MAX_LOCATION_UPDATES_PER_MINUTE)
  * @returns {boolean} True if update is allowed, false if rate limit exceeded
  * * @example
  * if (!checkRateLimit(socket.id)) {
  * log("Rate limit exceeded", "error");
  * return; // Reject the update
  * }
  */
 function checkRateLimit(socketId, maxPerMinute = MAX_LOCATION_UPDATES_PER_MINUTE) {
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
  * * Drivers that haven't sent an update in STALE_DRIVER_TIMEOUT milliseconds
  * are considered inactive and are removed from the drivers store.
  * This prevents memory leaks and keeps the data fresh.
  * * This function is called periodically by setInterval (see CLEANUP_INTERVAL).
  * * @returns {void}
  */
function cleanupStaleDrivers() {
  const now = Date.now();
  let cleaned = 0;
  for (const [accountId, driver] of Object.entries(drivers)) {
    const timeSinceUpdate = now - new Date(driver.lastUpdated).getTime();
    const timeSinceDisconnect = driver.disconnectedAt ? (now - driver.disconnectedAt) : 0;
    const isDisconnected = driver.disconnected === true;
    
    // Remove driver if:
    // 1. They haven't sent an update in STALE_DRIVER_TIMEOUT (5 minutes), AND
    // 2. Either:
    //    - They're not disconnected (no grace period to wait for), OR
    //    - They're disconnected AND grace period has expired
    const gracePeriodExpired = isDisconnected && timeSinceDisconnect > DISCONNECT_GRACE_PERIOD;
    
    if (timeSinceUpdate > STALE_DRIVER_TIMEOUT) {
      if (!isDisconnected || gracePeriodExpired) {
        // Clean up: Remove driver data and socket mapping
        delete drivers[accountId];
        if (driver.socketId) {
          delete socketToAccountId[driver.socketId];
        }
        cleaned++;
        
        if (IS_DEV) {
          const reason = isDisconnected ? 
            `(disconnected ${Math.round(timeSinceDisconnect/1000)}s ago, grace period expired, ${driver.reconnectAttempts || 0} reconnects)` :
            `(no updates for ${Math.round(timeSinceUpdate/1000)}s)`;
          log(`🗑️ Cleaned up stale driver ${accountId} ${reason}`);
        }
      } else if (IS_DEV) {
        // Driver is disconnected but still in grace period
        const remainingTime = Math.round((DISCONNECT_GRACE_PERIOD - timeSinceDisconnect) / 1000);
        const reconnectCount = driver.reconnectAttempts || 0;
        log(`⏳ [${accountId}] Disconnected driver in grace period (${remainingTime}s remaining, ${reconnectCount}/${MAX_RECONNECT_ATTEMPTS} reconnects)`);
      }
    } else if (isDisconnected && IS_DEV) {
      // Driver is disconnected but still receiving updates (shouldn't happen, but log for debugging)
      const remainingTime = Math.round((DISCONNECT_GRACE_PERIOD - timeSinceDisconnect) / 1000);
      if (remainingTime > 0) {
        log(`⏳ [${accountId}] Disconnected but has recent updates (${remainingTime}s remaining in grace period)`);
      }
    }
  }
  
  if (cleaned > 0 && IS_DEV) {
    console.log(`🧹 Cleaned up ${cleaned} stale driver(s)`);
  }
}
 
 /**
  * Conditional logging function that reduces console spam in production.
  * * In development mode, all logs are shown. In production, only errors are logged.
  * This improves performance and reduces log file size.
  * * @param {string} message - The message to log
  * @param {string} level - Log level: "info" (default) or "error"
  * @returns {void}
  * * @example
  * log("User connected", "info");
  * log("Critical error", "error"); // Always logged
  */
 function log(message, level = "info") {
   if (IS_DEV || level === "error") {
     console.log(message);
   }
 }
 
 /**
  * Validate location data received from clients.
  * * Ensures that the data object contains valid location information:
  * - Must be an object
  * - Must have a valid accountId (non-empty string)
  * - Latitude must be a number between -90 and 90 (required for location updates)
  * - Longitude must be a number between -180 and 180 (required for location updates)
  * * @param {object} data - The location data to validate
  * @returns {boolean} True if data is valid, false otherwise
  * * @example
  * if (!validateLocationData(data)) {
  * socket.emit("error", { message: "Invalid location data" });
  * return;
  * }
  */
 function validateLocationData(data) {
   if (!data || typeof data !== "object") return false;
   if (!data.accountId || typeof data.accountId !== "string") return false;
   // Coordinates are required for location updates
   if (data.lat === undefined || data.lng === undefined) return false;
   // Handle string coordinates (convert and validate)
   const lat = typeof data.lat === "string" ? parseFloat(data.lat) : data.lat;
   const lng = typeof data.lng === "string" ? parseFloat(data.lng) : data.lng;
   if (typeof lat !== "number" || isNaN(lat) || lat < -90 || lat > 90) return false;
   if (typeof lng !== "number" || isNaN(lng) || lng < -180 || lng > 180) return false;
   return true;
 }
 
 // ========== EXPRESS ROUTES ==========
 
 /**
  * Root endpoint - Server status and statistics
  * Returns basic server information and current driver count
  */
 app.get("/", (req, res) => {
   res.json({ 
     status: "running",
     drivers: Object.keys(drivers).length,
     uptime: process.uptime()
   });
 });
/**
 * Health check endpoint
 * Used by monitoring services to check if the server is alive
 */
app.get("/health", (req, res) => {
  res.json({ status: "healthy", timestamp: new Date().toISOString() });
});
 
 // ========== SOCKET.IO CONNECTION HANDLER ==========
 
 /**
  * Main Socket.IO connection handler
  * * This is called whenever a new client connects to the server.
  * It sets up event handlers for all socket events and manages
  * the connection lifecycle.
  * * Socket Events Handled:
  * - registerRole: Client declares itself as "user" or "driver"
  * - updateLocation: Driver sends location update (every 15 seconds)
  * - destinationUpdate: Driver updates destination
  * - routeUpdate: Driver updates route geometry
  * - passengerUpdate: Driver updates passenger count
  * - getBusInfo: User requests specific bus information
  * - requestDriversData: User requests all active drivers
  * - disconnect: Client disconnects
  * - error: Socket error occurred
  */
 io.on("connection", (socket) => {
   log(`✅ Client connected: ${socket.id}`);
   
   /**
    * Cleanup function called when a socket disconnects
    * Removes 
  the driver from memory and cleans up mappings
    */
  const cleanup = () => {
    const accountId = socketToAccountId[socket.id];
    if (accountId && drivers[accountId]) {
      // Check if this socket is the current active socket for this driver
      if (drivers[accountId].socketId === socket.id) {
        // Mark as disconnected but keep data for grace period
        drivers[accountId].disconnected = true;
        drivers[accountId].disconnectedAt = Date.now();
        drivers[accountId].socketId = null; // Clear socket ID
        log(`🔌 [${accountId}] Driver disconnected (grace period: ${DISCONNECT_GRACE_PERIOD/1000}s)`);
      }
      // If socket ID doesn't match, it means driver already reconnected with new socket
      // Just clean up this old socket mapping
    }
    delete socketToAccountId[socket.id];
    delete rateLimitMap[socket.id];
  };
 
   /**
    * Error handling wrapper for socket event handlers
    * Catches any errors and prevents them from crashing the server
    * * @param {string} eventName - Name of the event being handled
    * @param {Function} handler - The event handler function
    * @returns {Function} Wrapped handler function
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
 
 // --- ROLE REGISTRATION ---
   /**
    * registerRole Event Handler
    * * Clients must register their role ("user" or "driver") after connecting.
  * This allows the server to:
    * - Join the client to the appropriate Socket.IO room
    * - Send initial data snapshot to users
    * - Track which sockets are drivers vs users
    * * For users: Sends a batched snapshot of all active drivers (driversSnapshot event)
    * For drivers: Just joins the "driver" room
    * * @event registerRole
    * @param {string} role - The role: "user" or "driver"
    * * @emits driversSnapshot - Sent 
  to users with all active driver data
    */
   socket.on("registerRole", safeHandler("registerRole", (role) => {
     if (role !== "user" && role !== "driver") {
       log(`⚠️ Unknown role from ${socket.id}: ${role}`);
       return;
     }

    socket.role = role;
    socket.join(role);
    
    // Reset rate limit on role registration to allow grace period after reconnection
    // This prevents rate limiting when app reconnects and sends initial updates
    if (rateLimitMap[socket.id]) {
      delete rateLimitMap[socket.id];
    }
    
    log(`🆔 ${socket.id} registered as ${role}`);

    if (role === "user") {
      /**
       * OPTIMIZATION: Batch all driver data into a single message.
       * Instead of sending multiple individual events, we send one
       * driversSnapshot event with all driver data.
       * This reduces network overhead and improves performance.
       *
       * OPTIMIZATION: Limit snapshot size to prevent huge payloads
       * when there are many drivers.
       * If MAX_SNAPSHOT_DRIVERS is set, only send the most recently updated drivers.
       *
       * NOTE: lastUpdated is included temporarily for sorting purposes only.
       * It's stored server-side for cleanup but NOT sent to clients in the snapshot.
       */
      let driversArray = Object.values(drivers)
        .filter(driver => driver.accountId && (driver.lat || driver.geometry))
        .map(driver => ({
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

      // Limit snapshot size if configured (optimization for many drivers)
      if (MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS) {
        driversArray = driversArray
          .sort((a, b) => new Date(b.lastUpdated || 0) - new Date(a.lastUpdated || 0))
          .slice(0, MAX_SNAPSHOT_DRIVERS)
          .map(({ lastUpdated, ...driver }) => driver);
        log(`⚠️ Snapshot limited to ${MAX_SNAPSHOT_DRIVERS} of ${totalDrivers} drivers`);
      } else {
        driversArray = driversArray.map(({ lastUpdated, ...driver }) => driver);
      }

      // ✅ Send the standard driver snapshot (existing logic)
      socket.emit("driversSnapshot", {
        drivers: driversArray,
        count: driversArray.length,
        total: totalDrivers,
        limited: MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS,
      });

      // ✅ FIX: ensure late joiners immediately receive the full current driver data
      try {
        const lateJoinSnapshot = Object.values(drivers)
          .filter(driver => driver.accountId && (driver.lat || driver.geometry))
          .map(driver => ({
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

        log(`📤 Late joiner snapshot sent: ${lateJoinSnapshot.length} active driver(s) to user ${socket.id}`);
      } catch (err) {
        log(`❌ Error sending late joiner snapshot to ${socket.id}:`, err);
      }
    }
  }));
 
   // --- LOCATION UPDATES (Driver → Server → Users) ---
   /**
    * updateLocation Event Handler
    * * This is the core event handler for location updates.
  Drivers send this
    * event every 15 seconds with their current location.
  * * BROADCAST LOGIC (Important for 15-second intervals):
    * The server broadcasts location updates to users if ANY of these conditions are met:
    * * 1. First update: This is the driver's first location update
    * 2. Location changed: Bus moved more than LOCATION_CHANGE_THRESHOLD (~11 meters)
    * 3. Passenger data changed: Passenger count or capacity changed
    * 4. 15-second interval: 15+ seconds have passed since last broadcast (HEARTBEAT)
    * * The 15-second interval rule ensures that even if a 
  bus is stopped,
    * users still receive regular updates every 15 seconds.
  This is crucial
    * for maintaining real-time tracking and showing that the bus is active.
  * * DATA STORAGE:
    * Location data is ALWAYS stored in memory, even if it's not broadcast.
  * This ensures that getBusInfo and other requests always have the latest data.
  * * @event updateLocation
    * @param {object} data - Location data object
    * @param {string} data.accountId - Unique driver/bus identifier
    * @param {number} data.lat - Latitude
    * @param {number} data.lng - Longitude
    * @param {string} [data.organizationName] - Organization name
    * @param {string} [data.destinationName] - Destination name
    * @param {number} [data.destinationLat] - Destination latitude
    * @param {number} [data.destinationLng] - Destination longitude
    * @param {number} [data.passengerCount] - Current passenger count
    * @param {number} [data.maxCapacity] - Maximum bus capacity
   
   * * @emits locationUpdate - Broadcast to all users in "user" room
    * @emits error - Sent to client if data is invalid or rate limit exceeded
    */
   socket.on("updateLocation", safeHandler("updateLocation", (data) => {
     // Validate incoming data
     if (!validateLocationData(data)) {
       log(`❌ [${data?.accountId || socket.id}] Invalid location data`, "error");
       socket.emit("error", { message: "Invalid location data" });
       return;
     }

     // Rate limiting check - prevent abuse
     if (!checkRateLimit(socket.id, MAX_LOCATION_UPDATES_PER_MINUTE)) {
       const accountId = data?.accountId || socketToAccountId[socket.id] || "unknown";
       const role = socket.role || "unregistered";
       log(`⚠️ Rate limit exceeded for ${socket.id} (${accountId}, ${role}) - Too many updates sent`, "error");
       socket.emit("error", { message: "Rate limit exceeded. Please slow down location updates." });
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
      const timeDisconnected = prevDriver.disconnectedAt ? (now - prevDriver.disconnectedAt) : 0;
      
      // Clear disconnected status
      prevDriver.disconnected = false;
      prevDriver.disconnectedAt = null;
      prevDriver.reconnectAttempts = reconnectAttempts;
      
      // Handle socket ID change (driver reconnected with new socket)
      if (prevDriver.socketId && prevDriver.socketId !== socket.id) {
        // Clean up old socket mapping
        delete socketToAccountId[prevDriver.socketId];
        log(`🔄 [${accountId}] Reconnected (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}) - Socket changed: ${prevDriver.socketId} → ${socket.id} (was disconnected ${Math.round(timeDisconnected/1000)}s)`);
      } else {
        log(`🔄 [${accountId}] Reconnected (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS}) - Data preserved (was disconnected ${Math.round(timeDisconnected/1000)}s)`);
      }
    } else if (prevDriver && prevDriver.socketId && prevDriver.socketId !== socket.id) {
      // Socket ID changed but driver wasn't marked as disconnected (edge case)
      delete socketToAccountId[prevDriver.socketId];
      log(`🔄 [${accountId}] Socket ID changed: ${prevDriver.socketId} → ${socket.id}`);
    }
    
    // Check if coordinates changed (for logging movement)
    // Note: We compare against last BROADCASTED location for movement detection
     
     // Calculate time since last broadcast (Infinity if no previous broadcast)
     const timeSinceLastBroadcast = prevDriver?.lastBroadcastTime ?
       (now - prevDriver.lastBroadcastTime) : Infinity;
 
     /**
      * BROADCAST DECISION LOGIC
      * * We determine whether to broadcast based on multiple factors:
      */
     
     // 1. Check if location changed significantly (more than threshold)
     // Compare against last BROADCASTED location (lastLat/lastLng), not last received location
     const locationChanged = !prevDriver ||
       !prevDriver.lastLat || !prevDriver.lastLng ||
       calculateDistance(lat, lng, prevDriver.lastLat, prevDriver.lastLng) > LOCATION_CHANGE_THRESHOLD;
     // 2. Check if passenger data changed
     const passengerDataChanged = passengerCount !== prevDriver?.passengerCount || 
       maxCapacity !== prevDriver?.maxCapacity;
     /**
      * 3. Check if 15-second interval has passed (HEARTBEAT)
      * * This is the key rule for 15-second updates:
      * Even if the bus hasn't moved, we broadcast if 15+ seconds have passed.
  * This ensures users see regular updates and know the bus is active.
  */
     const isIntervalUpdate = timeSinceLastBroadcast >= LOCATION_UPDATE_INTERVAL;
     /**
      * Final broadcast decision:
      * Broadcast if it's the first update, location changed, passengers changed, or 15s passed
      */
     const shouldBroadcast = !prevDriver ||
       locationChanged || 
       passengerDataChanged || 
       isIntervalUpdate;
 
     /**
      * UPDATE DATA STORE
      * * Always update the in-memory store with the latest data, even if we don't broadcast.
  * This ensures that getBusInfo and other requests always have the most recent data.
  * * IMPORTANT: lastLat and lastLng should only be updated when we broadcast.
  * This ensures that locationChanged comparison is against the last BROADCASTED location,
      * not the last received location.
  This way, if a bus moves slightly but we don't broadcast
      * (because 15s hasn't passed), we still compare against the last broadcasted location,
      * and when 15s passes, we'll detect the movement and broadcast.
  */
     drivers[accountId] = {
       ...prevDriver,
       accountId,
       organizationName: organizationName ||
  prevDriver?.organizationName || "No Organization",
       destinationName: destinationName || prevDriver?.destinationName ||
  "Unknown",
       destinationLat: destinationLat ?? prevDriver?.destinationLat,
       destinationLng: destinationLng ??
  prevDriver?.destinationLng,
       lat,  // Current location (always updated)
       lng,  // Current location (always updated)
       passengerCount: passengerCount ??
  prevDriver?.passengerCount ?? 0,
       maxCapacity: maxCapacity ?? prevDriver?.maxCapacity ??
  0,
       lastUpdated: new Date().toISOString(),
       socketId: socket.id, // Update socket ID (handles reconnections)
       disconnected: false, // Ensure driver is marked as connected when receiving updates
       disconnectedAt: null, // Clear disconnect timestamp
       reconnectAttempts: prevDriver?.reconnectAttempts || 0, // Preserve reconnect attempts count
       // Only update lastLat/lastLng when we broadcast (these represent last broadcasted location)
       lastLat: shouldBroadcast ?
  lat : (prevDriver?.lastLat ?? lat),
       lastLng: shouldBroadcast ?
  lng : (prevDriver?.lastLng ?? lng),
       lastBroadcastTime: shouldBroadcast ? now : prevDriver?.lastBroadcastTime,
     };
     socketToAccountId[socket.id] = accountId;
 
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
         const distanceFromLast = calculateDistance(lat, lng, prevDriver.lastLat, prevDriver.lastLng) * 111000;
         log(`🚌 [${accountId}] Moved ${distanceFromLast.toFixed(0)}m → (${lat?.toFixed(6)}, ${lng?.toFixed(6)}) | Passengers: ${drivers[accountId].passengerCount}/${drivers[accountId].maxCapacity}`);
       } else if (passengerDataChanged) {
         log(`🚌 [${accountId}] Location: (${lat?.toFixed(6)}, ${lng?.toFixed(6)}) | Passengers changed: ${drivers[accountId].passengerCount}/${drivers[accountId].maxCapacity}`);
       } else {
         log(`🚌 [${accountId}] Location: (${lat?.toFixed(6)}, ${lng?.toFixed(6)}) | Passengers: ${drivers[accountId].passengerCount}/${drivers[accountId].maxCapacity} | Heartbeat`);
       }
     }
     // Note: We don't log updates that aren't broadcast (they're stored but not sent yet)
   }));
 
   // --- DESTINATION UPDATE ---
   /**
    * destinationUpdate Event Handler
    * * Drivers send this event when they change their destination.
  * The server immediately broadcasts this to all users.
    * * @event destinationUpdate
    * @param {object} data - Destination data
    * @param {string} data.accountId - Driver account ID
    * @param {string} [data.destinationName] - Destination name
    * @param {number} [data.destinationLat] - Destination latitude
    * @param {number} [data.destinationLng] - Destination longitude
    * * @emits destinationUpdate - Broadcast to all users
    * @emits error - Sent to client if accountId is missing
    */
   socket.on("destinationUpdate", safeHandler("destinationUpdate", (data) => {
    
     if (!data?.accountId) {
       socket.emit("error", { message: "Missing accountId" });
       return;
     }

     const { accountId, destinationName, destinationLat, destinationLng } = data;
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
       log(`🔄 [${accountId}] Reconnected via destinationUpdate (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
     }

     // Update driver data with new destination
     drivers[accountId] = {
       ...prev,
       accountId,
       destinationName: destinationName ?? prev.destinationName ?? "Unknown",
       destinationLat: destinationLat ??
  prev.destinationLat,
       destinationLng: destinationLng ?? prev.destinationLng,
       lastUpdated: new Date().toISOString(),
       socketId: socket.id,
       disconnected: false,
       disconnectedAt: null,
     };
     socketToAccountId[socket.id] = accountId;
 
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
     log(`🎯 [${accountId}] Destination updated: ${drivers[accountId].destinationName}`);
   }));
 
   // --- ROUTE UPDATE ---
   /**
    * routeUpdate Event Handler
    * * Drivers send this event when they update their route geometry.
    * Route geometry is typically a polyline or set of coordinates representing
    * the planned route.
    * * OPTIMIZATION: Only broadcasts and logs when route geometry or destination actually changes.
    * This prevents spam when the app sends frequent updates with the same values.
    * @event routeUpdate
    * @param {object} data - Route data
    * @param {string} data.accountId - Driver account ID
    * @param {object} data.geometry - Route geometry data (polyline, coordinates, etc.)
    * @param {number} [data.destinationLat] - Destination latitude
    * @param {number} [data.destinationLng] - Destination longitude
    * @emits routeUpdate - Broadcast to all users (only if route changed)
    * @emits error - Sent to client if accountId is missing
    */
   socket.on("routeUpdate", safeHandler("routeUpdate", (data) => {
  
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
      log(`🔄 [${accountId}] Reconnected via routeUpdate (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
    }

    // Check if route data actually changed
    const prevGeometry = prev.geometry;
    const geometryChanged = JSON.stringify(geometry) !== JSON.stringify(prevGeometry);
    const destinationLatChanged = destinationLat !== undefined && destinationLat !== prev.destinationLat;
    const destinationLngChanged = destinationLng !== undefined && destinationLng !== prev.destinationLng;
    const routeChanged = geometryChanged || destinationLatChanged || destinationLngChanged;

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
      
      // Log polyline change
      if (geometryChanged && geometry) {
        const polylineStr = typeof geometry === "string" ? geometry : JSON.stringify(geometry);
        log(`🗺️ [${accountId}] Route updated | Polyline changed: ${polylineStr}`);
      } else {
        log(`🗺️ [${accountId}] Route updated`);
      }
    }
    // If route didn't change, we silently update the data store without broadcasting/logging
   }));
 
   // --- PASSENGER COUNT UPDATE ---
   /**
    * passengerUpdate Event Handler
    * * Drivers send this event when the passenger count changes.
  * This is immediately broadcast to all users so they can see
    * how many seats are available on each bus.
  * * OPTIMIZATION: Only broadcasts and logs when passenger count or capacity actually changes.
  * This prevents log spam when the app sends frequent updates with the same values.
  * * @event passengerUpdate
    * @param {object} data - Passenger data
    * @param {string} data.accountId - Driver account ID
    * @param {number} [data.passengerCount] - Current passenger count
    * @param {number} [data.maxCapacity] - Maximum bus capacity
    * * @emits passengerUpdate - Broadcast to all users (only if values changed)
    * @emits error - Sent to client if accountId is missing
    */
   socket.on("passengerUpdate", safeHandler("passengerUpdate", (data) => {
     if (!data?.accountId) {
       socket.emit("error", { message: 
  "Missing accountId" });
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
       log(`🔄 [${accountId}] Reconnected via passengerUpdate (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
     }
     
     // Normalize values (use previous values if not provided)
     const newPassengerCount = passengerCount ?? prev.passengerCount ?? 0;
     const newMaxCapacity = maxCapacity ?? prev.maxCapacity ?? 0;
     const prevPassengerCount = prev.passengerCount ?? 0;
     const prevMaxCapacity = prev.maxCapacity ??
  0;

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
       log(`🧍 [${accountId}] Passenger count updated: ${drivers[accountId].passengerCount}/${drivers[accountId].maxCapacity}`);
     }
     // If values didn't change, we silently update the data store without broadcasting/logging
   }));
 
 // --- USER REQUEST: Get Specific Bus Info ---
   /**
    * getBusInfo Event Handler
    * * Users can request detailed information about a specific bus.
  * This is useful when a user wants to see all details about a particular bus.
  * * @event getBusInfo
    * @param {object} data - Request data
    * @param {string} data.accountId - Driver account ID to get info for
    * * @emits busInfo - Sent to requesting user with bus details
    * @emits busInfoError - Sent to requesting user if bus not found or accountId missing
    */
   socket.on("getBusInfo", safeHandler("getBusInfo", (data) => {
     const { accountId } = data || {};
     if (!accountId) {
       socket.emit("busInfoError", { message: "Missing accountId" 
  });
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
   }));
 
 // --- USER REQUEST: Get All Active Drivers ---
   /**
    * requestDriversData Event Handler
    * * Users can request a list of all active drivers and their locations.
  * This is useful for getting a quick snapshot of all buses.
  * * Note: This is different from the driversSnapshot sent on registration.
  * This can be requested at any time by an already-connected user.
  * * @event requestDriversData
    * @emits driversData - Sent to requesting user with all active drivers
    */
   socket.on("requestDriversData", safeHandler("requestDriversData", () => {
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
   }));
 
 // --- USER REQUEST: Get Current Data for Late Joiners (Snapshot Refresh) ---
 /**
  * requestCurrentData Event Handler
  * * Users can request the full state (location, destination, route geometry) 
  * of all active drivers at any time after registration.
  * This is crucial for:
  * 1. Late joiners who missed the initial 'driversSnapshot'.
  * 2. Users refreshing after a connection issue.
  * * It re-uses the logic from 'registerRole' to send a batched, optimized snapshot.
  * * @event requestCurrentData
  * @emits driversSnapshot - Sent to the requesting user only.
  */
 socket.on("requestCurrentData", safeHandler("requestCurrentData", () => {
     // Re-use the optimized snapshot generation logic from registerRole
     let driversArray = Object.values(drivers)
       // Filter for drivers with location OR geometry data
       .filter(driver => driver.accountId && (driver.lat || driver.geometry))
       .map(driver => ({
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
     if (MAX_SNAPSHOT_DRIVERS > 0 && driversArray.length > MAX_SNAPSHOT_DRIVERS) {
       // Sort by lastUpdated (most recent first) and take top N
       driversArray = driversArray
         .sort((a, b) => new Date(b.lastUpdated || 0) - new Date(a.lastUpdated || 0))
         .slice(0, MAX_SNAPSHOT_DRIVERS)
         .map(({ lastUpdated, ...driver }) => driver); // Remove lastUpdated
       log(`⚠️ Snapshot refresh limited to ${MAX_SNAPSHOT_DRIVERS} of ${totalDrivers} drivers`);
     } else {
       // Remove lastUpdated before sending to client (server-only field)
       driversArray = driversArray.map(({ lastUpdated, ...driver }) => driver);
     }
     
     // Emit the existing 'driversSnapshot' event to the requesting user only
     socket.emit("driversSnapshot", {
         drivers: driversArray,
         count: driversArray.length,
         total: totalDrivers, 
         limited: MAX_SNAPSHOT_DRIVERS > 0 && totalDrivers > MAX_SNAPSHOT_DRIVERS,
     });
     
     log(`📤 Sent requested snapshot of ${driversArray.length} driver(s) to user ${socket.id}`);
 }));
 
 
 // --- DISCONNECT HANDLER ---
   /**
    * disconnect Event Handler
    * * Called when a client disconnects.
  Cleans up driver data and mappings.
    */
   socket.on("disconnect", () => {
     log(`❌ Disconnected: ${socket.id} (${socket.role || "unknown"})`);
     cleanup();
   });
 /**
    * error Event Handler
    * * Called when a socket error occurs.
  Logs the error and cleans up.
    */
   socket.on("error", (error) => {
     log(`❌ Socket error for ${socket.id}: ${error.message}`, "error");
     cleanup();
   });
 });
 
 // ========== PERIODIC CLEANUP TASKS ==========
 
 /**
  * Periodic cleanup of stale drivers
  * * Runs every CLEANUP_INTERVAL (60 seconds) to remove drivers that haven't
  * sent updates in STALE_DRIVER_TIMEOUT (5 minutes).
  * * This prevents memory leaks and keeps the data store clean.
  */
 setInterval(cleanupStaleDrivers, CLEANUP_INTERVAL);
 /**
  * Periodic cleanup of rate limit map
  * * Runs every minute to remove expired rate limit entries.
  * This prevents the rateLimitMap from growing indefinitely.
  */
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
 // Bind to all interfaces (required for Render and other cloud platforms)
 
 /**
  * Start the server
  * * The server listens on the specified PORT and HOST.
  * On Render and other cloud platforms, the PORT may be set by the environment,
  * but we use a fixed port for simplicity.
  */
 server.listen(PORT, HOST, () => {
   console.log(`✅ Server running on ${HOST}:${PORT}`);
   console.log(`📊 Environment: ${IS_DEV ? "Development" : "Production"}`);
   console.log(`⚙️  Compression: Enabled`);
   console.log(`🧹 Cleanup interval: ${CLEANUP_INTERVAL / 1000}s`);
   console.log(`📍 Location update interval: ${LOCATION_UPDATE_INTERVAL / 1000}s (15-second heartbeat enabled)`);
 });
 
/**
 * Graceful shutdown handler
 * * Handles SIGTERM/SIGINT signals (sent by process managers like PM2, Docker, etc.)
 * to allow the server to shut down gracefully, closing connections properly.
 * * Steps:
 * 1. Mark all drivers as disconnected (trigger grace period)
 * 2. Notify all clients that server is shutting down
 * 3. Close Socket.IO server (closes all WebSocket connections)
 * 4. Close HTTP server
 * 5. Exit process
 * * NOTE: On server restart, all in-memory data is lost (expected behavior).
 * Drivers will automatically reconnect and restore their data by sending updates.
 */
function gracefulShutdown(signal) {
  console.log(`${signal} received, shutting down gracefully`);
  
  // Mark all active drivers as disconnected (preserve data for grace period)
  // NOTE: This only helps if server restarts within grace period. 
  // On full restart, all in-memory data is lost (this is expected behavior).
  const activeDrivers = Object.keys(drivers).filter(
    accountId => drivers[accountId] && !drivers[accountId].disconnected
  );
  
  if (activeDrivers.length > 0) {
    const now = Date.now();
    activeDrivers.forEach(accountId => {
      drivers[accountId].disconnected = true;
      drivers[accountId].disconnectedAt = now;
      drivers[accountId].socketId = null;
    });
    console.log(`🔌 Marked ${activeDrivers.length} driver(s) as disconnected`);
  }
  
  // Notify all clients that server is shutting down
  // Clients should automatically reconnect when server restarts
  try {
    io.emit("serverShutdown", { 
      message: "Server is shutting down. Reconnecting...",
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.log("⚠️ Error notifying clients:", error.message);
  }
  
  // Give clients a moment to receive the shutdown message
  setTimeout(() => {
    // Close Socket.IO server (closes all WebSocket connections properly)
    io.close(() => {
      console.log("Socket.IO server closed - all connections terminated");
      
      // Close HTTP server (stops accepting new connections)
      server.close(() => {
        console.log("HTTP server closed");
        console.log(`SERVER: Graceful shutdown complete`);
        console.log(`SERVER: NOTE: On server restart, all in-memory data will be lost.`);
        console.log(`SERVER: Drivers will need to reconnect and send updates to restore data.`);
        process.exit(0);
      });
    });
  }, 500); // 500ms grace period for clients to receive shutdown message
}

// Handle SIGTERM (production deployments, Docker, etc.)
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));

// Hand... (1 KB left)
