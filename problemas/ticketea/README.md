# Ticketea Scraper

Scraper para el sitio Ticketea.com.py que maneja la autenticación con Cloudflare Turnstile usando 2captcha.

## Características

- Bypass automático de Cloudflare Turnstile usando 2captcha
- Login automático con credenciales específicas
- Extracción de datos de eventos
- Logging detallado
- Guardado de datos en formato JSON

## Configuración

### Requisitos

```bash
pip install -r requirements.txt
```

### Variables de entorno

- **API Key 2captcha**: `af9e3756d516cdb21be7f0d4d5e83f8b`
- **Credenciales de login**: 
  - Email: `camila.halfon@daleplay.la`
  - Password: `turquesa_oro`

## Uso

### Ejecutar el scraper

```bash
python ticketea_scraper.py
```

## Funcionamiento

1. **Obtención de página de login**: El scraper accede a la página de login y extrae:
   - Sitekey del Cloudflare Turnstile
   - Token de autenticidad del formulario

2. **Resolución de Turnstile**: Usando la API de 2captcha:
   - Envía una tarea de tipo `TurnstileTaskProxyless`
   - Espera la resolución del captcha
   - Obtiene el token de respuesta

3. **Login**: Envía el formulario con:
   - Credenciales de usuario
   - Token de autenticidad
   - Token de Turnstile resuelto

4. **Scraping**: Una vez autenticado, accede al dashboard y extrae datos de eventos

## Estructura de archivos

```
ticketea/
├── ticketea_scraper.py          # Script principal
├── requirements.txt             # Dependencias
├── README.md                   # Documentación
├── ticketea_scraper.log        # Logs del scraper
└── jsonticketea/               # Datos extraídos en JSON
    └── ticketea_data_*.json
```

## Logs

El scraper genera logs detallados en `ticketea_scraper.log` y en la consola, incluyendo:
- Progreso del proceso de login
- Estado de resolución del Turnstile
- Errores y excepciones
- Datos extraídos

## API de 2captcha

### Configuración de Turnstile

El scraper utiliza la API de 2captcha para resolver el Cloudflare Turnstile:

```python
task_data = {
    "clientKey": "af9e3756d516cdb21be7f0d4d5e83f8b",
    "task": {
        "type": "TurnstileTaskProxyless",
        "websiteURL": "https://www.ticketea.com.py/manage/sign_in",
        "websiteKey": "0x4AAAAAAADWGgqdTg_SY-mN"
    }
}
```

### Proceso de resolución

1. Crear tarea en 2captcha
2. Polling cada 10 segundos para verificar estado
3. Obtener token cuando esté listo
4. Usar token en el formulario de login

## Notas técnicas

- **Sitekey**: `0x4AAAAAAADWGgqdTg_SY-mN` (extraído del HTML)
- **URL de login**: `https://www.ticketea.com.py/manage/sign_in`
- **Tiempo de espera**: Máximo 5 minutos para resolución de Turnstile
- **User Agent**: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)

## Troubleshooting

### Errores comunes

1. **Error de API 2captcha**: Verificar saldo y API key
2. **Timeout de Turnstile**: Incrementar tiempo de espera
3. **Error de login**: Verificar credenciales y tokens
4. **Bloqueo de IP**: Usar proxies si es necesario

### Debug

Para debug adicional, modificar el nivel de logging:

```python
logging.basicConfig(level=logging.DEBUG)
```
