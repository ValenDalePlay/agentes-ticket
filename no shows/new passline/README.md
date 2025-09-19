# Passline Scraper

Scraper simple para Passline con Chrome driver no detectable.

## Instalación

```bash
pip install -r requirements.txt
```

## Uso

```bash
python passline_scraper.py
```

## Características

- Chrome driver con opciones anti-detección básicas
- Login automático con credenciales configuradas
- Pausa para resolución manual de reCAPTCHA
- Extracción de datos del dashboard
- Guardado automático en formato JSON
- Navegador permanece abierto para inspección manual

## Credenciales

- Email: emilio@lauriaweb.com
- Password: 3nk5878651

## Notas

- El scraper pausa 30 segundos para permitir resolución manual del reCAPTCHA
- Los datos se guardan en la carpeta `jsonpassline/`
- El navegador permanece abierto al final para inspección manual
- Presiona Ctrl+C para cerrar el navegador

## Estructura de datos

Los datos se guardan en formato JSON con:
- timestamp
- page_title
- current_url
- elementos encontrados
- contenido principal extraído
