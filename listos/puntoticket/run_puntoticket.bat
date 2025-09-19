@echo off
echo ========================================
echo    PUNTO TICKET SCRAPER - FINAL
echo ========================================
echo.
echo Iniciando scraper...
echo.

cd /d "%~dp0"

python puntoticket_scraper.py

echo.
echo ========================================
echo Scraper completado - Cerrando...
echo ========================================
timeout /t 3 /nobreak >nul
exit