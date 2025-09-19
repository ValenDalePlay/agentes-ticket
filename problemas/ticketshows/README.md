# TicketShows Scraper

Este scraper automatiza la extracción de información de eventos desde la plataforma TicketShows.

## Características

- Login automático con credenciales configuradas
- Navegación automática al menú de Reportes
- Acceso a la sección "Venta Diaria"
- Análisis del desplegable de actividades
- Extracción de eventos disponibles
- Guardado de datos en formato JSON
- Capturas de pantalla para debugging
- Logging detallado de operaciones

## Instalación

1. Instalar las dependencias:
```bash
pip install -r requirements.txt
```

2. Asegurarse de tener Chrome instalado en el sistema.

## Uso

Ejecutar el scraper:
```bash
python ticketshows_scraper.py
```

## Configuración

### Credenciales
Las credenciales están configuradas en el archivo `ticketshows_scraper.py`:
- Usuario: `dukiec`
- Contraseña: `2025`

### Configuración del navegador
- Por defecto se ejecuta en modo visible (headless=False)
- Para ejecutar en modo headless, cambiar `headless=False` a `headless=True`

## Estructura de archivos

```
ticketshows/
├── ticketshows_scraper.py    # Script principal
├── requirements.txt          # Dependencias
├── README.md                # Este archivo
└── jsonticketshows/         # Carpeta para archivos JSON generados
```

## Funcionalidades

### Login automático
- Navega a la página de login de TicketShows
- Completa automáticamente los campos de usuario y contraseña
- Maneja errores de autenticación

### Navegación a Reportes
- Hace clic en el menú "Reportes"
- Selecciona "Venta Diaria"
- Espera 15 segundos para que cargue la página

### Análisis de Actividades
- Obtiene información del desplegable de actividades
- Cuenta el número total de actividades disponibles
- Extrae detalles de cada actividad (ID, nombre, estado)
- Abre el desplegable para verificación visual

### Extracción de Datos de Tablas
- Selecciona automáticamente cada actividad del desplegable
- Espera a que cargue la tabla de datos correspondiente
- Extrae todos los registros de la tabla de ventas diarias
- Identifica filas de totales automáticamente
- Guarda los datos en archivos JSON separados por actividad

### Extracción de eventos
- Busca eventos disponibles en la página después del login
- Extrae información como título, fecha y ubicación
- Guarda los datos en archivos JSON con timestamp

### Debugging
- Capturas de pantalla automáticas
- Logging detallado de todas las operaciones
- Análisis de elementos de la página

## Formato de salida

Los datos se guardan en archivos JSON con la siguiente estructura:

### Archivo de Datos de Tabla (ticketshows_tabla_*.json)
```json
{
  "fecha_extraccion": "2025-01-27T10:30:00",
  "actividad": {
    "id": "13586",
    "nombre": "Rels B en Quito"
  },
  "total_registros": 20,
  "datos_tabla": [
    {
      "indice": 1,
      "fecha": "27 - Nov",
      "tickets_vendidos": "8.262",
      "valor": "$602.535,49",
      "tickets_vendidos_acum": "8.262",
      "valor_acum": "$602.535,49",
      "es_total": false
    },
    {
      "indice": 20,
      "fecha": "TOTAL",
      "tickets_vendidos": "8.516",
      "valor": "$624.266,50",
      "tickets_vendidos_acum": "",
      "valor_acum": "",
      "es_total": true
    }
  ]
}
```

### Archivo de Eventos (ticketshows_data_*.json)
```json
{
  "fecha_extraccion": "2025-01-27T10:30:00",
  "total_eventos": 5,
  "eventos": [
    {
      "indice": 1,
      "titulo": "Nombre del Evento",
      "fecha": "27/01/2025",
      "ubicacion": "Lugar del evento",
      "texto": "Texto completo del elemento",
      "html": "<div>HTML del elemento</div>"
    }
  ]
}
```

## Troubleshooting

### Problemas comunes

1. **Error de ChromeDriver**: El scraper intenta descargar automáticamente el driver correcto
2. **Elementos no encontrados**: El scraper guarda capturas de pantalla para debugging
3. **Login fallido**: Verificar credenciales y conectividad de red

### Logs
Todos los logs se muestran en consola con timestamp y nivel de detalle.

## Notas

- El scraper está diseñado para ser robusto y manejar diferentes estructuras de página
- Se incluyen múltiples selectores CSS para encontrar elementos de eventos
- Las capturas de pantalla ayudan a debuggear problemas de navegación
