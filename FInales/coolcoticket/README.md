# Coolco Ticket Scraper

Este scraper automatiza la extracción de datos desde el sitio web de Coolco Ticket.

## Funcionalidades

- Navegación automática al sitio web de Coolco Ticket
- Extracción automática de datos de todas las tablas disponibles
- Análisis inteligente de la estructura de la página
- Guardado de datos en formato JSON
- Manejo robusto de errores y logging detallado
- Adaptación automática a diferentes estructuras de tablas

## Configuración

### Sitio web objetivo
- **URL principal**: https://www.coolcoticket.com
- **Tipo**: Sitio de venta de tickets/entradas
- **Datos a extraer**: Tablas de eventos, precios, disponibilidad

### Requisitos
- Python 3.8+
- Chrome browser instalado
- Dependencias listadas en requirements.txt

## Instalación

```bash
pip install -r requirements.txt
```

## Uso

### Ejecución básica
```bash
python coolcoticket_scraper.py
```

### Ejecución con URL personalizada
Al ejecutar el scraper, se te pedirá ingresar una URL específica o usar la URL por defecto.

### Ejecución programática
```python
from coolcoticket_scraper import CoolcoTicketScraper

scraper = CoolcoTicketScraper(headless=False)
success = scraper.run("https://www.coolcoticket.com/eventos")
```

## Estructura de archivos generados

Los archivos JSON se guardan en la carpeta `jsoncoolcoticket/` con el siguiente formato:

```
coolcoticket_data_YYYYMMDD_HHMMSS.json
```

### Estructura del JSON
```json
{
  "fuente": "Coolco Ticket",
  "url": "https://www.coolcoticket.com",
  "fecha_extraccion": "2025-08-29T17:30:00.000000",
  "total_registros": 25,
  "datos": [
    {
      "fuente": "Coolco Ticket",
      "tabla_numero": 1,
      "fila_numero": 1,
      "fecha_extraccion": "2025-08-29T17:30:00.000000",
      "total_columnas": 5,
      "evento": "Concierto X",
      "fecha": "2025-09-15",
      "precio": "$50.000",
      "disponibilidad": "Disponible",
      "ubicacion": "Teatro ABC"
    }
  ]
}
```

## Características técnicas

### Manejo de tablas
- **Detección automática**: Encuentra todas las tablas de la página
- **Headers flexibles**: Extrae headers de `<thead>` o primera fila
- **Normalización**: Convierte nombres de columnas a formato snake_case
- **Datos robustos**: Maneja celdas vacías y estructuras irregulares

### Logging y monitoreo
- **Logs detallados**: Información completa del proceso de extracción
- **Contadores**: Número de tablas, filas y registros procesados
- **Errores específicos**: Mensajes claros para debugging

### Opciones de configuración
- **Modo headless**: Para ejecución sin interfaz gráfica
- **Timeouts configurables**: Adaptables según la velocidad del sitio
- **User-Agent personalizado**: Para evitar bloqueos

## Ejemplos de uso

### Extraer eventos de una página específica
```python
scraper = CoolcoTicketScraper()
success = scraper.run("https://www.coolcoticket.com/eventos-musicales")
```

### Modo headless para automatización
```python
scraper = CoolcoTicketScraper(headless=True)
success = scraper.run("https://www.coolcoticket.com")
```

## Solución de problemas

### Error de conexión
- Verificar que la URL sea correcta y esté disponible
- Comprobar la conexión a internet

### No se encuentran tablas
- La página podría usar JavaScript para cargar contenido
- Verificar que el sitio tenga tablas HTML estándar

### Problemas de permisos
- Verificar permisos de escritura en la carpeta `jsoncoolcoticket/`
- Ejecutar con permisos de administrador si es necesario

## Mantenimiento

El scraper está diseñado para ser robusto y adaptarse a cambios menores en la estructura del sitio web. Para actualizaciones mayores:

1. Revisar los logs para identificar errores específicos
2. Ajustar los selectores CSS si es necesario
3. Actualizar la lógica de extracción según cambios en el sitio

## Contacto y soporte

Para reportar problemas o solicitar nuevas funcionalidades, revisar el código fuente y los logs generados.
