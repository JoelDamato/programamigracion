const fs = require("fs");
const Papa = require("papaparse");
const axios = require("axios");

// Configuración
const notionToken = "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";
const databaseClientesId = "14e482517a9581458d4bfefbcde4ea03";
const databaseInteraccionesId = "14e482517a9581cbbfa7e9fc3dd61bae";
const metricsId = "14e482517a9581f1ba44c86043cf23a0";

const closers = {
  "Carlos Tu": "f67663b3-b033-4a66-b119-f3c069666caa",
  "Jose Santiago": "13cd872b-594c-81a2-adbf-0002854c4356",
  "Matias Randazzo": "14dd872b-594c-81d9-917f-0002a255901b",
  "Walter Alegre": "87876e86-58f2-4b10-8b0a-67b15c55d59b",
  "Mauro Gaitan": "13cd872b-594c-81e6-9132-000280ded969",
  "Matias": "14cd872b-594c-81fd-b1b9-0002cf942368",
  "Yessica Ferreira": "10bd872b-594c-8167-a326-0002c1b0314e",
};

// Función para validar y formatear números de teléfono
function validatePhoneNumber(phone) {
  if (!phone) return null;
  const cleanPhone = phone.replace(/\D/g, "");
  if (cleanPhone.length === 10) {
    return `+549${cleanPhone}`;
  } else if (cleanPhone.length === 13 && cleanPhone.startsWith("549")) {
    return `+${cleanPhone}`;
  }
  return null;
}

// Manejo de reintentos con backoff
async function retryWithBackoff(fn, retries = 3, delay = 1000) {
  try {
    return await fn();
  } catch (error) {
    if (error.response?.status === 409 && retries > 0) {
      console.log(`Conflicto detectado. Reintentando en ${delay}ms...`);
      await new Promise((resolve) => setTimeout(resolve, delay));
      return retryWithBackoff(fn, retries - 1, delay * 2);
    }
    if (retries > 0) {
      console.log(`Error encontrado. Reintentando en ${delay}ms...`);
      await new Promise((resolve) => setTimeout(resolve, delay));
      return retryWithBackoff(fn, retries - 1, delay * 2);
    }
    throw error;
  }
}

// Procesamiento por lotes
async function batchProcess(data, batchSize = 5) {
  for (let i = 0; i < data.length; i += batchSize) {
    const batch = data.slice(i, i + batchSize);
    const promises = batch.map((client) => createClientsAndInteractions(client));
    await Promise.all(promises);
    await new Promise((resolve) => setTimeout(resolve, 500)); // Pausa entre lotes.
  }
}

// Crear clientes e interacciones
async function createClientsAndInteractions(client) {
  const clientId = await retryWithBackoff(() => searchClientByPhone(client.phone));

  if (!clientId) {
    const newClientId = await retryWithBackoff(() => createClient(client));
    if (newClientId) {
      await retryWithBackoff(() =>
        createInteraction(
          {
            channel: "WSP",
            response: "Sin respuesta",
            temperature: "Frio",
            contactType: "Inicial",
            state: "Pendiente",
            origin: client.origin,
          },
          newClientId
        )
      );
    }
  } else {
    await retryWithBackoff(() =>
      createInteraction(
        {
          channel: "WSP",
          response: "Sin respuesta",
          temperature: "Frio",
          contactType: "Inicial",
          state: "Pendiente",
          origin: client.origin,
        },
        clientId
      )
    );
  }
}

// Buscar cliente por teléfono
async function searchClientByPhone(phone) {
  try {
    const response = await axios.post(
      `https://api.notion.com/v1/databases/${databaseClientesId}/query`,
      {
        filter: {
          property: "Telefono",
          phone_number: { equals: phone },
        },
      },
      {
        headers: {
          Authorization: `Bearer ${notionToken}`,
          "Notion-Version": "2022-06-28",
          "Content-Type": "application/json",
        },
      }
    );
    return response.data.results.length > 0 ? response.data.results[0].id : null;
  } catch (error) {
    console.error("Error buscando cliente:", error.response?.data || error.message);
    return null;
  }
}

// Crear cliente en Notion
const conflictLog = [];
async function createClient(data) {
  try {
    const existingClientId = await searchClientByPhone(data.phone);
    if (existingClientId) {
      console.log(`Cliente con teléfono ${data.phone} ya existe.`);
      return existingClientId;
    }

    const response = await axios.post(
      "https://api.notion.com/v1/pages",
      {
        parent: { database_id: databaseClientesId },
        properties: {
          Nombre: { title: [{ text: { content: "Nuevo cliente" } }] },
          Telefono: { phone_number: data.phone },
          ...(data.group && { "Grupo Whatsapp": { select: { name: data.group } } }),
          ...(data.closer && { Closer: { people: [{ id: data.closer }] } }),
          Metricas: { relation: [{ id: metricsId }] },
        },
      },
      {
        headers: {
          Authorization: `Bearer ${notionToken}`,
          "Notion-Version": "2022-06-28",
          "Content-Type": "application/json",
        },
      }
    );
    console.log("Cliente creado en Notion:", response.data.id);
    return response.data.id;
  } catch (error) {
    if (error.response?.status === 409) {
      conflictLog.push(data);
      console.error("Conflicto detectado. Cliente no creado:", data.phone);
    } else {
      console.error("Error creando cliente:", error.response?.data || error.message);
    }
    return null;
  }
}

// Crear interacción en Notion
async function createInteraction(data, clientId) {
  if (!clientId) {
    console.error("Error: No se puede crear interacción sin un ID de cliente válido.");
    return;
  }

  try {
    const response = await axios.post(
      "https://api.notion.com/v1/pages",
      {
        parent: { database_id: databaseInteraccionesId },
        properties: {
          "Nombre cliente": { relation: [{ id: clientId }] },
          "Estado interaccion": { select: { name: "Finalizada" } },
          Origen: { select: { name: data.origin } },
          Metricas: { relation: [{ id: metricsId }] },
        },
      },
      {
        headers: {
          Authorization: `Bearer ${notionToken}`,
          "Notion-Version": "2022-06-28",
          "Content-Type": "application/json",
        },
      }
    );
    console.log("Interacción creada en Notion:", response.data.id);
  } catch (error) {
    console.error("Error creando interacción:", error.response?.data || error.message);
  }
}

// Procesar el archivo CSV
async function processCSV() {
  const ignoredRows = [];
  const cleanData = [];

  try {
    const fileContent = fs.readFileSync("scrapeo.csv", "utf8");
    Papa.parse(fileContent, {
      skipEmptyLines: true,
      header: true,
      complete: async (results) => {
        results.data.forEach((row) => {
          const phone =
            row["Telefonos prospección"]?.trim() ||
            row["Telefonos prospecci�n"]?.trim() ||
            row["Telefono"]?.trim();
          const origin = row["Origen"]?.trim() || "Desconocido";
          const validatedPhone = validatePhoneNumber(phone);

          if (!validatedPhone) {
            ignoredRows.push({ row, reason: "Número de teléfono inválido" });
            return;
          }

          cleanData.push({
            phone: validatedPhone,
            group: row["Grupo Whatsapp"]?.trim() || null,
            closer: closers[row["Closer"]?.trim()] || null,
            origin: origin,
          });
        });

        console.log("Datos normalizados:", cleanData);

        await batchProcess(cleanData, 5);

        fs.writeFileSync("ignored_rows.json", JSON.stringify(ignoredRows, null, 2));
        fs.writeFileSync("conflict_log.json", JSON.stringify(conflictLog, null, 2));
        console.log("Reporte de filas ignoradas y conflictos guardado.");
      },
    });
  } catch (error) {
    console.error("Error procesando CSV:", error.message);
  }
}

// Ejecutar el script
processCSV();
