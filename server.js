const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const fs = require("fs").promises;
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = socketIo(server, { cors: { origin: "*" } });

const PORT = 3000;
const DB_PATH = path.join(__dirname, "database.json");
const BACKUP_PATH = path.join(__dirname, "database.backup.json");

// ---------- Mutex Lock ----------
let dbLock = false;

async function acquireLock() {
  while (dbLock) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  dbLock = true;
}

function releaseLock() {
  dbLock = false;
}

// ---------- Data Access & Auto Backup ----------
async function readInvoices() {
  await acquireLock();
  try {
    const data = await fs.readFile(DB_PATH, "utf8");
    return JSON.parse(data);
  } catch (err) {
    if (err.code === "ENOENT") {
      await fs.writeFile(DB_PATH, JSON.stringify([], null, 2));
      return [];
    }
    throw err;
  } finally {
    releaseLock();
  }
}

async function writeInvoices(invoices) {
  await acquireLock();
  try {
    const data = JSON.stringify(invoices, null, 2);
    await fs.writeFile(DB_PATH, data);
    await fs.writeFile(BACKUP_PATH, data);
  } finally {
    releaseLock();
  }
}

// ---------- Middlewares ----------
app.use(express.json({ limit: '50mb' })); // Allow large Base64 Logo uploads
app.use(express.static(__dirname));

// ---------- Secure REST Routes ----------
app.get("/api/invoices", async (req, res) => {
  try {
    const invoices = await readInvoices();
    res.json(invoices);
  } catch (err) {
    res.status(500).json({ error: "Failed to read database." });
  }
});

app.post("/api/invoices", async (req, res) => {
  try {
    const { id, company, client, service, price, date, status, tax, discount, notes, logo, total } = req.body;

    // Strict Validation
    if (!id || !company || !client || !service || typeof price !== "number") {
      return res.status(400).json({ error: "Missing required fields." });
    }

    const invoices = await readInvoices();
    const newInvoice = {
      id,
      company,
      client,
      service,
      price,
      date: date || new Date().toLocaleDateString("en-GB"),
      status: status || 'Pending',
      tax: tax || 0,
      discount: discount || 0,
      notes: notes || '',
      logo: logo || null,
      total: total || price
    };

    invoices.unshift(newInvoice);
    await writeInvoices(invoices);

    io.emit("invoices-updated", invoices);
    res.status(201).json(newInvoice);
  } catch (err) {
    res.status(500).json({ error: "Failed to save invoice." });
  }
});

app.put("/api/invoices/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { company, client, service, price, status, tax, discount, notes, logo, total } = req.body;

    let invoices = await readInvoices();
    const index = invoices.findIndex((inv) => inv.id === id);

    if (index === -1) {
      return res.status(404).json({ error: "Invoice not found." });
    }

    // Apply strict updates across ALL fields
    invoices[index] = {
      ...invoices[index],
      company,
      client,
      service,
      price,
      date: date || invoices[index].date,
      status: status || invoices[index].status || "Pending",
      tax: typeof tax === "number" ? tax : 0,
      discount: typeof discount === "number" ? discount : 0,
      notes: notes || "",
      logo: logo || null,
      total: typeof total === "number" ? total : price,
    };

    await writeInvoices(invoices);
    io.emit("invoices-updated", invoices);
    res.json(invoices[index]);
  } catch (err) {
    res.status(500).json({ error: "Failed to update invoice." });
  }
});

app.delete("/api/invoices/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const invoices = await readInvoices();

    const filteredInvoices = invoices.filter((inv) => inv.id !== id);

    if (filteredInvoices.length === invoices.length) {
      return res.status(404).json({ error: "Invoice not found." });
    }

    await writeInvoices(filteredInvoices);
    io.emit("invoices-updated", filteredInvoices);
    res.json({ message: "Deleted successfully." });
  } catch (err) {
    res.status(500).json({ error: "Failed to delete invoice." });
  }
});

// ---------- WebSocket Sync ----------
io.on("connection", async (socket) => {
  try {
    const invoices = await readInvoices();
    socket.emit("invoices-updated", invoices);
  } catch (err) {
    console.error("Socket syncing issue:", err);
  }
});

server.listen(PORT, () => {
  console.log(`🚀 Premium Enterprise Server online: http://localhost:${PORT}`);
});