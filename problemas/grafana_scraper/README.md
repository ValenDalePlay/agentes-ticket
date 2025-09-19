# Grafana Dashboard Scraper

Scraper automatizado para extraer datos de dashboards de Grafana con autenticación SSO de AWS IAM Identity Center.

## Características

- **Login Automático SSO**: Se conecta automáticamente usando AWS IAM Identity Center
- **Extracción Masiva**: Procesa todos los dashboards disponibles automáticamente
- **Simulación Humana**: Implementa delays aleatorios y movimientos de mouse para evitar detección
- **Múltiples Formatos**: Guarda datos en JSON (completo) y CSV (métricas principales)
- **Logging Completo**: Registra todo el proceso para debugging

## Instalación

1. Instalar dependencias:
```bash
pip install -r requirements.txt
```

2. Asegurarse de tener Chrome instalado en el sistema.

## Uso

```bash
python grafana_scraper.py
```

## Proceso Automatizado

1. **Login SSO**: Navega a la página de login y hace clic en "Sign in with AWS IAM Identity Center"
2. **Extracción de URLs**: Obtiene todas las URLs de dashboards de la página principal
3. **Procesamiento Individual**: Para cada dashboard:
   - Navega a la URL específica
   - Extrae datos de paneles, métricas y tablas
   - Guarda en formatos JSON y CSV
4. **Comportamiento Humano**: Simula delays aleatorios y movimientos naturales

## Estructura de Datos Extraídos

### JSON (Completo)
```json
{
  "title": "Nombre del Dashboard",
  "url": "URL completa",
  "extracted_at": "timestamp ISO",
  "panels": [
    {
      "index": 0,
      "type": "chart|table|stat",
      "title": "Título del Panel",
      "data": {...},
      "text_content": [...]
    }
  ],
  "metrics": {
    "contexto": ["valor1", "valor2"]
  },
  "tables": [
    {
      "headers": [...],
      "rows": [[...]]
    }
  ]
}
```

### CSV (Métricas Principales)
| Dashboard | Panel | Metric_Type | Value | Context |
|-----------|-------|-------------|-------|---------|
| Dashboard Name | Panel Name | numeric/text/table_headers/table_row | Valor | Tipo |

## Archivos de Salida

- **JSON**: `jsongrafana/{dashboard_name}_{timestamp}.json`
- **CSV**: `jsongrafana/{dashboard_name}_{timestamp}.csv`
- **Log**: `grafana_scraper.log`

## Configuración

El scraper está configurado para:
- URL base: `https://g-cc18438941.grafana-workspace.us-east-2.amazonaws.com`
- Timeouts: 15-30 segundos para elementos críticos
- Delays humanos: 1-10 segundos entre acciones
- User Agent: Chrome en macOS

## Manejo de Errores

- Reintentos automáticos en elementos no encontrados
- Logging detallado de todos los errores
- Continuación del proceso aunque fallen dashboards individuales
- Cleanup automático del driver al finalizar

## Simulación Humana

- Movimientos aleatorios del mouse
- Delays variables entre acciones
- Scroll suave hacia elementos
- User agent realista
- Configuración anti-detección de webdriver

## Notas Importantes

- Requiere completar manualmente el proceso de SSO la primera vez
- Los datos extraídos dependen de los permisos del usuario autenticado
- El scraper respeta la estructura actual de Grafana (puede requerir ajustes si cambia la UI)
