const fs = require("fs");
const Papa = require("papaparse");
const axios = require("axios");

// Configuración
const notionToken = "ntn_GG849748837abCQnsctJHEtwe9JNDxoKbjkD61zGuqO02D";
const databaseClientesId = "128032a62365817cb2aef2c4c2b20179";
const databaseInteraccionesId = "128032a623658194afe7c59b5d3d3d67";
const metricsId = "128032a6236581f59b7bf8993198b037";

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

// Función para procesar el archivo CSV
async function processCSV() {
  const ignoredRows = [];
  const cleanData = [];

  try {
    const fileContent = fs.readFileSync("ghlnoaplica.csv", "utf8");
    Papa.parse(fileContent, {
      skipEmptyLines: true,
      header: true,
      complete: async (results) => {
        console.log("Contenido original del CSV:", results.data);

        results.data.forEach((row, index) => {
          console.log(`Procesando fila ${index + 1}:`, row);

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

        for (const client of cleanData) {
          const clientId = await searchClientByPhone(client.phone);
          if (!clientId) {
            const newClientId = await createClient(client);
            if (newClientId) {
              await createInteraction(
                {
                  channel: "WSP",
                  response: "Sin respuesta",
                  temperature: "Frio",
                  contactType: "Inicial",
                  state: "Pendiente",
                  origin: client.origin,
                },
                newClientId
              );
            }
          } else {
            await createInteraction(
              {
                channel: "WSP",
                response: "Sin respuesta",
                temperature: "Frio",
                contactType: "Inicial",
                state: "Pendiente",
                origin: client.origin,
              },
              clientId
            );
          }
        }

        fs.writeFileSync("ignored_rows.json", JSON.stringify(ignoredRows, null, 2));
        console.log("Reporte de filas ignoradas guardado en 'ignored_rows.json'.");
        console.log("Procesamiento completado.");
      },
    });
  } catch (error) {
    console.error("Error procesando CSV:", error.message);
  }
}

// Función para buscar cliente por teléfono
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

// Función para crear un cliente en Notion
async function createClient(data) {
  try {
    const response = await axios.post(
      "https://api.notion.com/v1/pages",
      {
        parent: { database_id: databaseClientesId },
        properties: {
          "Nombre": { title: [{ text: { content: "Nuevo cliente" } }] },
          "Telefono": { phone_number: data.phone },
          ...(data.group && { "Grupo Whatsapp": { rich_text: [{ text: { content: data.group } }] } }),
          ...(data.closer && { "Closer": { people: [{ id: data.closer }] } }),
          "Metricas": { relation: [{ id: metricsId }] },
          "Fecha creado": { date: { start: new Date().toISOString() } },
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
    console.error("Error creando cliente:", error.response?.data || error.message);
    return null;
  }
}

// Función para crear una interacción en Notion
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
          "Canal": { select: { name: "GHL" } },
          "Respuesta": { select: { name: "Respondio" } },
          "Tipo contacto": { select: { name: "Generado por usuario" } },
          "Estado interaccion": { select: { name: "Finalizada" } },
          "Agendamiento": { checkbox: true },
          "Origen": { select: { name: data.origin } },
          "Aplica?": { select: { name: "No aplica"} },
          "Producto de interes": { multi_select: [{ name: "MEG" }] },
          "Metricas": { relation: [{ id: metricsId }] },
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

// Ejecutar el script
processCSV();
