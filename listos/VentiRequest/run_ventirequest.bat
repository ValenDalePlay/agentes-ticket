@echo off
echo ========================================
echo    VENTI REQUEST SCRAPER
echo ========================================
echo.

REM Cambiar al directorio del script
cd /d "%~dp0"

REM Verificar que Python esté instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no está instalado o no está en el PATH
    echo Por favor instala Python y vuelve a intentar
    timeout /t 5 /nobreak >nul
    exit /b 1
)

REM Verificar que el archivo del scraper existe
if not exist "venti_request_scraper.py" (
    echo ERROR: No se encontró venti_request_scraper.py
    echo Asegúrate de estar en el directorio correcto
    timeout /t 5 /nobreak >nul
    exit /b 1
)

echo Iniciando Venti Request Scraper...
echo.

REM Ejecutar el scraper con guardado en base de datos
python venti_request_scraper.py --no-save --preview --save-db

echo.
echo ========================================
echo    SCRAPER COMPLETADO
echo ========================================
echo.
echo Cerrando automaticamente en 3 segundos...
timeout /t 3 /nobreak >nul
