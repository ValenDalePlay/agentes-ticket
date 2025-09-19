# Venti Request Scraper

Scraper para obtener datos de eventos de Venti usando su API REST.

## Funcionalidades

- Autenticación con la API de Venti
- **Detección automática de eventos disponibles** (sin necesidad de especificar IDs)
- Obtención de datos de eventos específicos
- Guardado automático de datos en formato JSON con timestamp
- Logging completo de operaciones
- Exploración inteligente de múltiples endpoints para encontrar eventos

## Instalación

```bash
pip install -r requirements.txt
```

## Uso

### Modo Automático (Recomendado)

El scraper detecta automáticamente los eventos disponibles:

```bash
python venti_request_scraper.py
```

### Modo Manual

Si conoces los IDs específicos de eventos, puedes editarlos en `venti_request_scraper.py`:

```python
EVENT_IDS = [123, 456, 789]  # Reemplazar con los IDs reales
```

Luego ejecutar:

```bash
python venti_request_scraper.py
```

## Configuración

El scraper usa las siguientes credenciales por defecto:
- Email: `fatima.villegas@daleplay.la`
- Password: `@Daleplay2025`

## Endpoints utilizados

1. **Autenticación**: `POST https://venti.com.ar/api/authenticate`
   - Body: `{"mail": "email", "password": "password", "platform": "web"}`
   - Retorna: token y datos del usuario

2. **Detección de eventos** (automática): Explora múltiples endpoints:
   - `GET /api/events`
   - `GET /api/user/events`
   - `GET /api/dashboard`
   - `GET /api/report/events`
   - `GET /api/my-events`
   - `GET /api/user/dashboard`
   - `GET /api/user/{userId}/events`

3. **Datos de evento**: `GET https://venti.com.ar/api/report/event/:idEvento`
   - Headers: `Authorization: Bearer {token}`
   - Retorna: datos completos del evento

## Archivos generados

Los datos se guardan en la carpeta `jsonventi/` con los siguientes formatos:
- `venti_auth_YYYYMMDD_HHMMSS.json`: Datos de autenticación
- `venti_events_response_{endpoint}_YYYYMMDD_HHMMSS.json`: Respuestas de endpoints de eventos (debug)
- `venti_event_{id}_YYYYMMDD_HHMMSS.json`: Datos de evento individual
- `venti_all_events_YYYYMMDD_HHMMSS.json`: Datos consolidados de múltiples eventos

## Logs

Los logs se guardan en `venti_request_scraper.log` y también se muestran en consola.
