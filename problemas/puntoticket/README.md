# Punto Ticket Scraper

## Descripción
Scraper automatizado para extraer datos de ventas de eventos de la plataforma Punto Ticket.

## Funcionalidades
- **Login automático**: Credenciales mgiglio/FMJBBWT
- **Extracción de eventos**: Itera por todos los eventos disponibles en el dropdown
- **Datos de ventas**: Extrae tablas de cantidad y monto de ventas
- **Filtrado temporal**: Solo guarda eventos con fechas actuales o futuras
- **Guardado JSON**: Datos consolidados en formato JSON

## Credenciales
- **Usuario**: mgiglio
- **Contraseña**: FMJBBWT
- **URL**: https://backoffice.puntoticket.com/Report?title=1.%20Resumen%20Ventas&reporteId=4&eventoId=BIZ183

## Instalación
```bash
pip install -r requirements.txt
```

## Uso
```bash
python puntoticket_scraper.py
```

## Estructura de datos extraídos
- Evento código y nombre
- Tablas de cantidad de ventas por categoría
- Tablas de monto de ventas por categoría
- Fechas de eventos filtradas (solo futuras/actuales)
- Metadata de extracción

## Archivos generados
Los datos se guardan en la carpeta `jsonpuntoticket/` con timestamp.
