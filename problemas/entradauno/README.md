# EntradaUno Scraper

Este scraper automatiza la extracción de datos de bordereaux desde el sistema EntradaUno.

## Características

- **Login Automático**: Se conecta automáticamente con las credenciales especificadas
- **Navegación Inteligente**: Navega automáticamente a la página de Bordeaux
- **Extracción Completa**: Extrae datos de todas las tablas disponibles:
  - Tabla principal de promociones y ventas
  - Tabla de resumen por descuentos
  - Tabla de formas de pago
  - Totales y estadísticas generales
- **Iteración Automática**: Itera por todas las combinaciones de:
  - Establecimientos
  - Espectáculos
  - Funciones
- **Almacenamiento JSON**: Guarda todos los datos en archivos JSON estructurados

## Requisitos

- Python 3.7+
- Chrome Browser
- ChromeDriver (se maneja automáticamente)

## Instalación

```bash
pip install -r requirements.txt
```

## Uso

```bash
python entradauno_scraper.py
```

## Credenciales

El scraper está configurado con las siguientes credenciales:
- **Usuario**: fLauria
- **Contraseña**: lauria2021

## URLs

- **Login**: https://bo.entradauno.com/Home/Login?ReturnUrl=/Reporte/General/HistoricoDeVentas
- **Bordeaux**: https://bo.entradauno.com/Reporte/General/BorderauxEtiquetaPrecio

## Funcionamiento

1. **Inicio de sesión**: El scraper accede a la página de login y se autentica automáticamente
2. **Navegación**: Se dirige a la página de Bordeaux
3. **Selección de filtros**: Para cada establecimiento disponible:
   - Selecciona el establecimiento
   - Para cada espectáculo en ese establecimiento:
     - Selecciona el espectáculo
     - Para cada función en ese espectáculo:
       - Selecciona la función
       - Espera 5 segundos para la carga de datos
       - Extrae todos los datos de las tablas
       - Guarda los datos en JSON

## Estructura de Datos Extraídos

```json
{
  "timestamp": "2025-01-XX...",
  "establecimiento": "Nombre del establecimiento",
  "espectaculo": "Nombre del espectáculo",
  "funcion": "Fecha y hora de la función",
  "evento_info": {
    "nombre_evento": "...",
    "locacion_fecha": "..."
  },
  "tabla_principal": [
    {
      "promocion": "...",
      "capacidad": "...",
      "vendidas": "...",
      "invitaciones": "...",
      "bloqueadas": "...",
      "kills": "...",
      "disponibles": "...",
      "total": "..."
    }
  ],
  "tabla_resumen": [
    {
      "descuento": "...",
      "ingresos": "...",
      "total": "..."
    }
  ],
  "formas_pago": [
    {
      "forma_pago": "...",
      "cantidad_tickets": "...",
      "total": "..."
    }
  ],
  "totales": {
    "vendidas": "...",
    "disponibles": "...",
    "invitaciones": "...",
    "capacidad": "...",
    "bloqueadas": "...",
    "kills": "...",
    "devueltas": "...",
    "monto_total": "...",
    "monto_total_devuelto": "...",
    "venta_total_neta": "..."
  }
}
```

## Archivos de Salida

Los archivos JSON se guardan en la carpeta `jsonentradauno/` con el formato:
```
entradauno_[ESTABLECIMIENTO]_[ESPECTACULO]_[TIMESTAMP].json
```

## Logs

El scraper genera logs detallados en:
- Archivo: `entradauno_scraper.log`
- Consola: Output en tiempo real

## Características Especiales

- **Espera Inteligente**: El scraper espera 5 segundos después de cada selección para asegurar la carga completa de datos
- **Manejo de Errores**: Continúa con la siguiente combinación si falla una extracción específica
- **Logging Detallado**: Registra cada paso del proceso para facilitar el debugging
- **Iteración Completa**: No se detiene hasta procesar todas las combinaciones posibles

## Troubleshooting

1. **Error de login**: Verificar credenciales y conexión a internet
2. **ChromeDriver**: Se maneja automáticamente, pero verificar que Chrome esté instalado
3. **Timeouts**: Aumentar los tiempos de espera si la conexión es lenta
4. **Elementos no encontrados**: Verificar que la estructura de la página no haya cambiado

## Notas

- El scraper respeta los tiempos de carga del sistema
- Se ejecuta de forma secuencial para evitar sobrecargar el servidor
- Todos los datos se extraen de forma estructurada y consistente
