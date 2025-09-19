from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
import os
from datetime import datetime, timedelta
import json
import re

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PuntoTicketScraper:
    def __init__(self, headless=False):
        """
        Inicializa el scraper de Punto Ticket
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
        """
        self.driver = None
        self.headless = headless
        self.download_folder = "jsonpuntoticket"
        
        # Credenciales de login
        self.username = "mgiglio"
        self.password = "FMJBBWT"
        self.login_url = "https://backoffice.puntoticket.com/Report?title=1.%20Resumen%20Ventas&reporteId=4&eventoId=BIZ183"
        
        # Crear carpeta de descarga si no existe
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
            logger.info(f"Carpeta creada: {self.download_folder}")
        else:
            logger.info(f"Carpeta ya existe: {self.download_folder}")
    
    def setup_driver(self):
        """Configura el driver de Chrome con las opciones necesarias"""
        try:
            logger.info("Configurando driver de Chrome...")
            
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
                logger.info("Modo headless activado")
            
            # Opciones adicionales para estabilidad
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Configurar descarga de archivos
            prefs = {
                "download.default_directory": os.path.abspath(self.download_folder),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Instalar ChromeDriver automáticamente con configuración específica
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con ChromeDriverManager: {e}")
                logger.info("Intentando usar ChromeDriver del sistema...")
                # Intentar usar ChromeDriver del sistema
                self.driver = webdriver.Chrome(options=chrome_options)
            
            # Configurar timeouts
            self.driver.implicitly_wait(10)
            self.driver.set_page_load_timeout(30)
            
            logger.info("Driver de Chrome configurado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error configurando el driver: {str(e)}")
            return False
    
    def login(self):
        """Realiza el login en el sistema Punto Ticket"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Navegar a la página de login
            logger.info(f"Navegando a: {self.login_url}")
            self.driver.get(self.login_url)
            time.sleep(3)
            
            # Buscar el formulario de login
            try:
                login_form = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "logon-form"))
                )
                logger.info("Formulario de login encontrado")
            except:
                logger.error("No se encontró el formulario de login")
                return False
            
            # Ingresar usuario
            try:
                user_field = self.driver.find_element(By.ID, "UserName")
                user_field.clear()
                user_field.send_keys(self.username)
                logger.info(f"Usuario ingresado: {self.username}")
            except:
                logger.error("No se encontró el campo de usuario")
                return False
            
            # Ingresar contraseña
            try:
                password_field = self.driver.find_element(By.ID, "Password")
                password_field.clear()
                password_field.send_keys(self.password)
                logger.info("Contraseña ingresada")
            except:
                logger.error("No se encontró el campo de contraseña")
                return False
            
            # Hacer clic en el botón "Ingresar"
            try:
                submit_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                submit_button.click()
                logger.info("Botón 'Ingresar' clickeado")
                time.sleep(5)
            except:
                logger.error("No se encontró el botón 'Ingresar'")
                return False
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL actual después del login: {current_url}")
            
            # Si ya no estamos en una página de login, el login fue exitoso
            if "logon" not in current_url.lower() and "login" not in current_url.lower():
                logger.info("Login exitoso")
                return True
            else:
                # Verificar si hay mensajes de error
                try:
                    error_element = self.driver.find_element(By.CLASS_NAME, "validation-summary-valid")
                    if error_element.is_displayed():
                        logger.error("Error de login detectado")
                        return False
                except:
                    pass
                
                logger.warning("El login puede no haber sido exitoso")
                return False
                
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def get_all_events(self):
        """Obtiene todos los eventos disponibles del dropdown"""
        try:
            logger.info("Obteniendo lista de eventos...")
            
            # Buscar el select de eventos
            try:
                event_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "eventoId"))
                )
                logger.info("Dropdown de eventos encontrado")
            except:
                logger.error("No se encontró el dropdown de eventos")
                return []
            
            # Obtener todas las opciones
            select = Select(event_select)
            options = select.options
            
            events = []
            for option in options:
                event_value = option.get_attribute("value")
                event_text = option.text.strip()
                
                # Saltar la opción vacía
                if event_value and event_text:
                    events.append({
                        "value": event_value,
                        "text": event_text
                    })
            
            logger.info(f"Encontrados {len(events)} eventos")
            return events
            
        except Exception as e:
            logger.error(f"Error obteniendo eventos: {str(e)}")
            return []
    
    def select_event_and_get_report(self, event_value, event_text):
        """Selecciona un evento y obtiene el reporte"""
        try:
            logger.info(f"Procesando evento: {event_text}")
            
            # Seleccionar el evento
            try:
                event_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "eventoId"))
                )
                select = Select(event_select)
                select.select_by_value(event_value)
                logger.info(f"Evento seleccionado: {event_value}")
                time.sleep(2)
            except:
                logger.error(f"No se pudo seleccionar el evento {event_value}")
                return None
            
            # Hacer clic en "Ver reporte"
            try:
                report_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[ng-click='vm.onBtnGetData()']"))
                )
                report_button.click()
                logger.info("Botón 'Ver reporte' clickeado")
                time.sleep(5)
            except:
                logger.error("No se encontró el botón 'Ver reporte'")
                return None
            
            # Esperar a que se carguen las tablas
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-stats"))
                )
                logger.info("Tablas de reporte cargadas")
            except:
                logger.warning("No se encontraron tablas de reporte para este evento")
                return None
            
            # Extraer datos de las tablas
            return self.extract_table_data(event_value, event_text)
            
        except Exception as e:
            logger.error(f"Error procesando evento {event_text}: {str(e)}")
            return None
    
    def extract_table_data(self, event_value, event_text):
        """Extrae datos de las tablas de cantidad y monto de ventas"""
        try:
            logger.info("Extrayendo datos de las tablas...")
            
            # Buscar todas las tablas
            tables = self.driver.find_elements(By.CSS_SELECTOR, "table.table-stats")
            
            if len(tables) < 1:
                logger.warning(f"No se encontraron tablas de reporte")
                return None
            
            logger.info(f"Encontradas {len(tables)} tablas")
            
            extracted_data = {
                "evento_codigo": event_value,
                "evento_nombre": event_text,
                "fecha_extraccion": datetime.now().isoformat(),
                "cantidad_ventas": [],
                "monto_ventas": []
            }
            
            # Procesar todas las tablas encontradas
            for i, table in enumerate(tables):
                table_type = "Cantidad Ventas" if i == 0 else f"Monto Ventas" if i == 1 else f"Tabla {i+1}"
                logger.info(f"Procesando tabla {i+1}: {table_type}")
                
                table_data = self.process_table(table, table_type)
                if table_data:
                    if i == 0:
                        extracted_data["cantidad_ventas"] = table_data
                    elif i == 1:
                        extracted_data["monto_ventas"] = table_data
                    else:
                        # Para tablas adicionales
                        extracted_data[f"tabla_{i+1}"] = table_data
            
            # Log de debug para ver qué datos tenemos
            logger.info(f"Datos extraídos antes del filtro:")
            logger.info(f"- Cantidad ventas: {len(extracted_data.get('cantidad_ventas', []))} filas")
            logger.info(f"- Monto ventas: {len(extracted_data.get('monto_ventas', []))} filas")
            
            # Mostrar algunas fechas para debug
            for row in extracted_data.get("cantidad_ventas", [])[:3]:
                logger.info(f"Muestra fecha cantidad: {row.get('fecha_evento', 'N/A')}")
            
            # Filtrar por fechas futuras o actuales
            filtered_data = self.filter_future_events(extracted_data)
            
            if not filtered_data:
                logger.info(f"Evento {event_text} no tiene fechas futuras o actuales, se omite")
                return None
            
            logger.info(f"Datos extraídos para evento: {event_text}")
            return filtered_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de tablas: {str(e)}")
            return None
    
    def process_table(self, table, table_type):
        """Procesa una tabla específica y extrae los datos"""
        try:
            # Extraer headers
            headers = []
            try:
                thead = table.find_element(By.TAG_NAME, "thead")
                header_row = thead.find_element(By.TAG_NAME, "tr")
                header_cells = header_row.find_elements(By.TAG_NAME, "th")
                headers = [cell.text.strip() for cell in header_cells]
            except:
                logger.warning(f"No se encontraron headers para tabla {table_type}")
                return []
            
            # Extraer filas de datos
            rows_data = []
            try:
                tbody = table.find_element(By.TAG_NAME, "tbody")
                data_rows = tbody.find_elements(By.TAG_NAME, "tr")
                
                for row in data_rows:
                    cells = row.find_elements(By.CSS_SELECTOR, "td")
                    
                    if cells and len(cells) > 0:
                        row_data = {
                            "tipo_tabla": table_type,
                            "fecha_extraccion": datetime.now().isoformat()
                        }
                        
                        # Mapear cada celda con su header correspondiente
                        for i, cell in enumerate(cells):
                            header_name = headers[i] if i < len(headers) else f"Columna_{i+1}"
                            cell_text = cell.text.strip()
                            
                            # Normalizar el nombre del header
                            header_key = header_name.lower().replace(" ", "_").replace("ñ", "n")
                            header_key = re.sub(r'[^\w]', '_', header_key)
                            header_key = re.sub(r'_+', '_', header_key).strip('_')
                            
                            row_data[header_key] = cell_text if cell_text else ""
                        
                        rows_data.append(row_data)
            
            except Exception as e:
                logger.warning(f"Error procesando filas de tabla {table_type}: {str(e)}")
            
            return rows_data
            
        except Exception as e:
            logger.error(f"Error procesando tabla {table_type}: {str(e)}")
            return []
    
    def filter_future_events(self, event_data):
        """Filtra eventos que tengan fechas actuales o futuras"""
        try:
            today = datetime.now().date()
            has_future_events = False
            
            logger.info(f"Filtrando fechas. Hoy es: {today}")
            
            # Verificar si hay fechas futuras en cantidad_ventas
            filtered_cantidad = []
            for row in event_data.get("cantidad_ventas", []):
                fecha_evento_str = row.get("fecha_evento", "")
                fecha_evento = self.parse_event_date(fecha_evento_str)
                
                if fecha_evento:
                    logger.info(f"Fecha encontrada: {fecha_evento_str} -> {fecha_evento} (¿Futura? {fecha_evento >= today})")
                    if fecha_evento >= today:
                        filtered_cantidad.append(row)
                        has_future_events = True
                else:
                    logger.debug(f"Fecha no válida o total: {fecha_evento_str}")
            
            # Verificar si hay fechas futuras en monto_ventas
            filtered_monto = []
            for row in event_data.get("monto_ventas", []):
                fecha_evento_str = row.get("fecha_evento", "")
                fecha_evento = self.parse_event_date(fecha_evento_str)
                
                if fecha_evento:
                    if fecha_evento >= today:
                        filtered_monto.append(row)
                        has_future_events = True
            
            if not has_future_events:
                logger.info(f"No se encontraron fechas futuras para el evento {event_data.get('evento_nombre', 'N/A')}")
                return None
            
            # Retornar datos filtrados
            filtered_data = event_data.copy()
            filtered_data["cantidad_ventas"] = filtered_cantidad
            filtered_data["monto_ventas"] = filtered_monto
            
            logger.info(f"Evento filtrado: {len(filtered_cantidad)} filas de cantidad, {len(filtered_monto)} filas de monto")
            return filtered_data
            
        except Exception as e:
            logger.error(f"Error filtrando eventos futuros: {str(e)}")
            return event_data  # Retornar datos originales si hay error
    
    def parse_event_date(self, date_string):
        """Parsea la fecha del evento del formato encontrado en las tablas"""
        try:
            if not date_string:
                return None
            
            # Ignorar filas de totales
            if "total" in date_string.lower() or date_string.strip().lower() == "total":
                return None
            
            # Formato esperado: "2025-09-05 21:00"
            date_part = date_string.split(" ")[0]  # Obtener solo la parte de la fecha
            
            # Validar que sea una fecha válida (formato YYYY-MM-DD)
            if len(date_part) == 10 and date_part.count("-") == 2:
                event_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                return event_date
            else:
                return None
            
        except Exception as e:
            logger.debug(f"No se pudo parsear la fecha: {date_string} - {str(e)}")
            return None
    
    def save_data_to_json(self, all_data, filename_prefix="puntoticket_data"):
        """Guarda los datos extraídos en un archivo JSON"""
        try:
            if not all_data:
                logger.warning("No hay datos para guardar")
                return None
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            json_filename = f"{filename_prefix}_{timestamp}.json"
            json_filepath = os.path.join(self.download_folder, json_filename)
            
            # Preparar datos para JSON
            json_data = {
                "fuente": "Punto Ticket",
                "url_login": self.login_url,
                "fecha_extraccion": datetime.now().isoformat(),
                "total_eventos_procesados": len(all_data),
                "eventos": all_data
            }
            
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Datos guardados en: {json_filepath}")
            return json_filepath
            
        except Exception as e:
            logger.error(f"Error guardando datos en JSON: {str(e)}")
            return None
    
    def close_driver(self):
        """Cierra el driver del navegador"""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Driver cerrado")
        except Exception as e:
            logger.error(f"Error cerrando el driver: {str(e)}")
    
    def run(self):
        """Ejecuta el scraper completo"""
        try:
            logger.info("=== INICIANDO SCRAPER DE PUNTO TICKET ===")
            
            # Configurar driver
            logger.info("PASO 1: Configurando driver...")
            if not self.setup_driver():
                logger.error("No se pudo configurar el driver")
                return False
            
            # Realizar login
            logger.info("PASO 2: Realizando login...")
            if not self.login():
                logger.error("No se pudo realizar el login")
                return False
            
            # Obtener lista de eventos
            logger.info("PASO 3: Obteniendo lista de eventos...")
            events = self.get_all_events()
            
            if not events:
                logger.error("No se pudieron obtener los eventos")
                return False
            
            # Procesar cada evento
            logger.info(f"PASO 4: Procesando {len(events)} eventos...")
            all_extracted_data = []
            
            for i, event in enumerate(events):
                logger.info(f"Procesando evento {i+1}/{len(events)}: {event['text']}")
                
                event_data = self.select_event_and_get_report(event['value'], event['text'])
                
                if event_data:
                    all_extracted_data.append(event_data)
                    logger.info(f"✅ Datos extraídos para: {event['text']}")
                else:
                    logger.info(f"⏭️ Sin datos futuros para: {event['text']}")
                
                # Pequeña pausa entre eventos
                time.sleep(2)
            
            # Guardar todos los datos
            if all_extracted_data:
                logger.info("PASO 5: Guardando datos...")
                json_file = self.save_data_to_json(all_extracted_data)
                
                if json_file:
                    logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
                    logger.info(f"Total de eventos con datos futuros: {len(all_extracted_data)}")
                    logger.info(f"Archivo JSON creado: {json_file}")
                    return True
                else:
                    logger.error("Error guardando los datos")
                    return False
            else:
                logger.warning("No se extrajeron datos de ningún evento")
                return False
                
        except Exception as e:
            logger.error(f"Error general en el scraper: {str(e)}")
            return False
        finally:
            # Cerrar navegador automáticamente
            logger.info("Cerrando navegador automáticamente...")
            self.close_driver()

def main():
    """Función principal"""
    logger.info("=== PUNTO TICKET SCRAPER AUTOMÁTICO ===")
    logger.info("Usuario: mgiglio")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://backoffice.puntoticket.com/Report...")
    logger.info("🎭 Solo eventos con fechas actuales o futuras")
    
    # Crear y ejecutar el scraper automáticamente
    scraper = PuntoTicketScraper(headless=False)
    success = scraper.run()
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Error en la ejecución del scraper")

if __name__ == "__main__":
    main()
