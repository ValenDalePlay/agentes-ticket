# Ticketmaster B2B Scraper

Scraper automatizado para la plataforma B2B de Ticketmaster que utiliza autenticación OAuth2.

## Características

- **Autenticación OAuth2**: Maneja el flujo completo de autenticación OAuth2 de Ticketmaster
- **Scraping de Eventos**: Extrae información detallada de eventos disponibles
- **Reportes de Ventas**: Obtiene datos de reportes de ventas cuando están disponibles
- **Detección Anti-Bot**: Utiliza undetected-chromedriver para evitar detección
- **Logging Detallado**: Sistema de logging completo para debugging
- **Guardado Automático**: Guarda datos en formato JSON con timestamps

## Requisitos

- Python 3.7+
- Chrome/Chromium instalado
- Credenciales válidas de Ticketmaster B2B

## Instalación

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Configurar credenciales en el archivo `ticketmaster_scraper.py`:
```python
username = "tu_usuario"
password = "tu_password"
```

## Uso

### Ejecución básica
```bash
python ticketmaster_scraper.py
```

### Uso como módulo
```python
from ticketmaster_scraper import TicketmasterB2BScraper

scraper = TicketmasterB2BScraper()
results = scraper.scrape_complete("username", "password")
```

## Flujo de Autenticación

El scraper maneja automáticamente el flujo OAuth2 de Ticketmaster:

1. **Paso 1**: Navega a la URL de autorización OAuth2
2. **Paso 2**: Ingresa el identificador de usuario y hace clic en "Siguiente"
3. **Paso 3**: Ingresa la contraseña y hace clic en "Verificar"
4. **Paso 4**: Maneja la redirección a `one.ticketmaster.com`
5. **Paso 5**: Extrae cookies de sesión para requests posteriores

## URL de Autenticación

```
https://b2bid-login.ticketmaster.com/oauth2/aus1ubr9idCrHoAOq697/v1/authorize?response_type=code&client_id=0ob7nisx9eQYkJZsb2e5&scope=profile%20openid%20email&state=...&redirect_uri=https://one.ticketmaster.com/login/oauth2/code/okta
```

## Datos Extraídos

### Eventos
- Título del evento
- Fecha y hora
- Venue/Lugar
- Precios
- Estado del evento
- Timestamp de extracción

### Reportes de Ventas
- Datos tabulares de ventas
- Métricas de performance
- Información de transacciones

## Archivos de Salida

Los datos se guardan en la carpeta `jsonticketmaster/` con los siguientes formatos:

- `ticketmaster_b2b_complete_YYYYMMDD_HHMMSS.json`: Datos completos
- `ticketmaster_b2b_events_YYYYMMDD_HHMMSS.json`: Solo eventos
- `ticketmaster_b2b_sales_YYYYMMDD_HHMMSS.json`: Solo reportes de ventas

## Debugging

El scraper genera archivos de debug:
- `debug_ticketmaster_final.html`: HTML de la página final para análisis

## Configuración Avanzada

### Opciones del Driver
```python
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--window-size=1920,1080')
options.add_argument('--disable-blink-features=AutomationControlled')
```

### Timeouts
- Login: 20 segundos
- Navegación: 5 segundos
- Extracción de datos: Variable según contenido

## Manejo de Errores

El scraper incluye manejo robusto de errores:
- Reintentos automáticos para elementos no encontrados
- Múltiples selectores CSS/XPath como fallback
- Logging detallado de errores para debugging
- Guardado de estado en caso de falla

## Limitaciones

- Requiere interfaz gráfica (no funciona en servidores headless sin configuración adicional)
- Dependiente de la estructura HTML de Ticketmaster (puede requerir actualizaciones)
- Sujeto a límites de rate limiting de la plataforma

## Troubleshooting

### Error de Login
- Verificar credenciales
- Revisar si hay captcha o verificación adicional
- Comprobar logs para detalles del error

### No se encuentran eventos
- Verificar que la cuenta tenga acceso a eventos
- Revisar selectores CSS/XPath en caso de cambios en la UI
- Comprobar archivo de debug HTML

### Errores de Chrome
- Actualizar undetected-chromedriver
- Verificar que Chrome esté instalado y actualizado
- Revisar permisos del sistema

## Soporte

Para problemas o mejoras, revisar:
1. Logs de ejecución
2. Archivos de debug HTML
3. Estructura actual de la página web
4. Cambios en la API de Ticketmaster
