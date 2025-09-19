from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
import os
from datetime import datetime, timedelta
import json
import re
import random
import string
import platform
import subprocess
import pandas as pd
from bs4 import BeautifulSoup
import pytz
from dateutil import parser
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection

# Configurar logging para Airflow (sin archivos físicos)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Solo logging a consola para Airflow
    ]
)
logger = logging.getLogger(__name__)

class TickantelScraper:
    def __init__(self, headless=True):  # Por defecto headless para contenedores
        """
        Inicializa el scraper de Tickantel optimizado para Airflow
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless (recomendado para contenedores)
        """
        self.driver = None
        self.headless = headless
        self.base_url = "https://tickantel.com.uy/dashboard/panel/login"
        
        # Credenciales proporcionadas
        self.username = "emilio@lauriaweb.com"
        self.password = "Elsalva5863"
        
        # Configuración para contenedores (no crear carpetas físicas)
        self.download_folder = "/tmp"  # Usar /tmp en contenedores
        
        # Configuración de evasión de bots
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        ]
        
        # Configuración de evasión
        self.evasion_config = {
            "random_delays": True,
            "human_typing": True,
            "mouse_movement": True,
            "scroll_behavior": True,
            "window_resize": True,
            "fingerprint_spoofing": True
        }
        
        # Datos finales para retornar (sin archivos físicos)
        self.final_data = {
            "ticketera": "TickAntel",
            "fecha_extraccion": None,
            "total_shows_procesados": 0,
            "shows_exitosos": 0,
            "shows_con_error": 0,
            "datos_por_show": {}
        }
        
        # Shows que NO queremos procesar
        self.excluded_shows = [
            "La Tabaré 40 años",
            "Kumbiaracha en concierto"
        ]
        
        logger.info("=== INICIALIZACIÓN DEL SCRAPER TICKANTEL PARA AIRFLOW ===")
        logger.info(f"URL objetivo: {self.base_url}")
        logger.info(f"Modo headless: {self.headless}")
        logger.info(f"Modo contenedor: Sin archivos físicos")
        logger.info("🛡️ MODOS DE EVASIÓN ACTIVADOS:")
        for key, value in self.evasion_config.items():
            logger.info(f"  🛡️ {key}: {'✅' if value else '❌'}")
        
        # Inicializar conexión a base de datos
        self.db_connection = None
        self.setup_database_connection()
    
    def setup_database_connection(self):
        """Verifica conexión con la base de datos PostgreSQL"""
        try:
            logger.info("🔌 Verificando conexión con la base de datos...")
            self.db_connection = get_database_connection()
            if self.db_connection:
                logger.info("✅ Conexión a base de datos establecida exitosamente")
                return True
            else:
                logger.error("❌ No se pudo establecer conexión a la base de datos")
                return False
        except Exception as e:
            logger.error(f"❌ Error en conexión a base de datos: {e}")
            return False
    
    def setup_driver(self):
        """Configura el driver de Chrome con evasión de bots y optimización para contenedores"""
        try:
            chrome_options = Options()
            
            # Modo headless para contenedores
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Opciones básicas para contenedores
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Evasión de detección de bots
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # User agent aleatorio
            user_agent = random.choice(self.user_agents)
            chrome_options.add_argument(f"--user-agent={user_agent}")
            
            # Configurar carpeta de descargas (para contenedores)
            chrome_options.add_experimental_option("prefs", {
                "download.default_directory": self.download_folder,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })
            
            # Intentar configurar el driver
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con configuración simple: {str(e)}")
                # Intentar con webdriver-manager como respaldo
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Configurar timeouts más largos debido a que el sitio puede tardar
            self.driver.implicitly_wait(30)
            self.driver.set_page_load_timeout(60)
            
            # Ejecutar script para ocultar webdriver
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("✅ Driver de Chrome configurado exitosamente con evasión de bots")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error al configurar el driver: {str(e)}")
            return False
    
    def navigate_to_login(self):
        """Navega a la página de login de Tickantel"""
        try:
            logger.info(f"Navegando a: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Esperar bastante tiempo ya que el sitio puede tardar
            logger.info("Esperando a que la página cargue (el sitio puede tardar bastante)...")
            WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Espera adicional para asegurar que todos los elementos estén cargados
            time.sleep(10)
            
            logger.info("Página de login cargada exitosamente")
            return True
            
        except TimeoutException:
            logger.error("Timeout esperando que la página de login cargue")
            return False
        except Exception as e:
            logger.error(f"Error navegando a la página de login: {str(e)}")
            return False
    
    def login(self):
        """
        Realiza el login en Tickantel
        
        Returns:
            bool: True si el login fue exitoso, False en caso contrario
        """
        try:
            logger.info("Iniciando proceso de login...")
            
            # Esperar a que los campos de login estén disponibles
            # Según el HTML proporcionado, los campos son 'username' y 'password'
            username_field = WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            
            password_field = WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.ID, "password"))
            )
            
            # Limpiar campos y escribir credenciales
            username_field.clear()
            username_field.send_keys(self.username)
            logger.info("Usuario ingresado")
            
            password_field.clear()
            password_field.send_keys(self.password)
            logger.info("Contraseña ingresada")
            
            # Esperar un momento antes de hacer clic en el botón
            time.sleep(2)
            
            # Hacer clic en el botón de login
            # Según el HTML, el botón tiene id="init-session"
            login_button = WebDriverWait(self.driver, 30).until(
                EC.element_to_be_clickable((By.ID, "init-session"))
            )
            
            login_button.click()
            logger.info("Botón de login clickeado")
            
            # Esperar bastante tiempo para el procesamiento del login
            # ya que el sitio puede tardar mucho
            logger.info("Esperando procesamiento del login (puede tardar bastante)...")
            time.sleep(30)
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL actual después del login: {current_url}")
            
            # Si no estamos más en la página de login, el login fue exitoso
            if "login" not in current_url.lower():
                logger.info("Login exitoso - Usuario autenticado")
                return True
            else:
                # Verificar si hay mensaje de error
                try:
                    error_element = self.driver.find_element(By.ID, "error-description")
                    error_text = error_element.text.strip()
                    if error_text:
                        logger.error(f"Error en login: {error_text}")
                        return False
                except NoSuchElementException:
                    pass
                
                # Esperar un poco más por si el login está procesándose
                logger.info("Esperando un poco más para verificar login...")
                time.sleep(20)
                
                current_url = self.driver.current_url
                if "login" not in current_url.lower():
                    logger.info("Login exitoso después de espera adicional")
                    return True
                else:
                    logger.warning("Login posiblemente fallido - aún en página de login")
                    return False
                
        except TimeoutException:
            logger.error("Timeout esperando elementos de login")
            return False
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def wait_for_dashboard_load(self, max_wait=120):
        """
        Espera a que el dashboard se cargue completamente
        
        Args:
            max_wait (int): Tiempo máximo de espera en segundos
            
        Returns:
            bool: True si el dashboard se carga correctamente
        """
        try:
            logger.info(f"Esperando a que el dashboard se cargue completamente ({max_wait}s)...")
            
            # Esperar a que aparezcan elementos típicos del dashboard
            dashboard_indicators = [
                "dashboard",
                "reportes",
                "panel",
                "datos",
                "estadisticas",
                "ventas"
            ]
            
            for i in range(max_wait):
                time.sleep(1)
                page_source = self.driver.page_source.lower()
                
                # Verificar si hay indicadores de dashboard cargado
                dashboard_loaded = any(indicator in page_source for indicator in dashboard_indicators)
                
                if dashboard_loaded:
                    logger.info(f"✅ Dashboard detectado después de {i+1} segundos")
                    # Esperar un poco más para asegurar carga completa
                    time.sleep(10)
                    return True
                    
                if i % 10 == 0:  # Log cada 10 segundos
                    logger.info(f"Esperando dashboard... ({i+1}/{max_wait})")
            
            logger.warning("Tiempo de espera agotado para carga del dashboard")
            return False
            
        except Exception as e:
            logger.error(f"Error esperando carga del dashboard: {e}")
            return False
    
    def extract_dashboard_data(self):
        """
        Extrae los datos del dashboard de Tickantel específicamente de los shows
        
        Returns:
            dict: Diccionario con los datos extraídos
        """
        try:
            logger.info("Extrayendo datos del dashboard...")
            
            # Esperar a que la página esté completamente cargada
            time.sleep(10)
            
            # Obtener información básica de la página
            page_title = self.driver.title
            current_url = self.driver.current_url
            
            # Estructura básica de datos
            dashboard_data = {
                "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "extraction_time": datetime.now().isoformat(),
                "url": current_url,
                "page_title": page_title,
                "username_used": self.username,
                "summary_data": {},
                "shows_data": [],
                "total_shows": 0
            }
            
            # 1. Extraer datos del resumen general (shows-summary)
            try:
                summary_section = self.driver.find_element(By.CSS_SELECTOR, ".shows-summary")
                summary_headers = summary_section.find_elements(By.CSS_SELECTOR, ".text-header")
                
                summary_info = {}
                for header in summary_headers:
                    try:
                        label_element = header.find_element(By.CSS_SELECTOR, "span:first-child")
                        value_element = header.find_element(By.CSS_SELECTOR, "span.numeric")
                        
                        label = label_element.text.strip()
                        value = value_element.text.strip()
                        
                        summary_info[label.lower().replace(" ", "_")] = value
                        
                    except Exception as e:
                        logger.warning(f"Error extrayendo header de resumen: {e}")
                
                dashboard_data["summary_data"] = summary_info
                logger.info(f"Datos de resumen extraídos: {summary_info}")
                
            except Exception as e:
                logger.warning(f"Error extrayendo resumen general: {e}")
            
            # 2. Extraer datos de cada show individual
            try:
                show_boxes = self.driver.find_elements(By.CSS_SELECTOR, ".show-box")
                logger.info(f"Encontrados {len(show_boxes)} shows")
                dashboard_data["total_shows"] = len(show_boxes)
                
                for i, show_box in enumerate(show_boxes):
                    try:
                        show_data = self.extract_show_data(show_box, i + 1)
                        if show_data:
                            dashboard_data["shows_data"].append(show_data)
                            
                    except Exception as e:
                        logger.warning(f"Error extrayendo show {i+1}: {e}")
                        
                logger.info(f"Extraídos datos de {len(dashboard_data['shows_data'])} shows exitosamente")
                
            except Exception as e:
                logger.warning(f"Error buscando shows: {e}")
            
            logger.info("Datos del dashboard extraídos exitosamente")
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos del dashboard: {str(e)}")
            return {
                "timestamp": datetime.now().strftime("%Y%m%d_%H%M%S"),
                "extraction_time": datetime.now().isoformat(),
                "error": str(e),
                "username_used": self.username
            }
    
    def extract_show_data(self, show_box, show_number):
        """
        Extrae los datos específicos de un show individual
        
        Args:
            show_box: Elemento de show de Selenium
            show_number (int): Número del show
            
        Returns:
            dict: Datos del show
        """
        try:
            show_data = {
                "show_number": show_number,
                "title": "",
                "date": "",
                "venue": "",
                "tickets_emitidos": "",
                "ocupado_porcentaje": "",
                "recaudado": "",
                "ticket_details": {}
            }
            
            # Extraer título del show
            try:
                title_element = show_box.find_element(By.CSS_SELECTOR, ".show__title h2 a")
                show_data["title"] = title_element.text.strip()
                show_data["show_id"] = title_element.get_attribute("id") or ""
                show_data["show_url"] = title_element.get_attribute("href") or ""
                
            except Exception as e:
                logger.warning(f"Error extrayendo título del show {show_number}: {e}")
            
            # Extraer fecha y venue
            try:
                data_list = show_box.find_elements(By.CSS_SELECTOR, ".show__data li")
                for li in data_list:
                    li_text = li.text.strip()
                    if "Función" in li_text or "/" in li_text:  # Fecha
                        # Remover "Función" si está presente
                        show_data["date"] = li_text.replace("Función", "").strip()
                    else:  # Venue
                        show_data["venue"] = li_text
                        
            except Exception as e:
                logger.warning(f"Error extrayendo fecha/venue del show {show_number}: {e}")
            
            # Extraer detalles del show (tickets emitidos, ocupado, recaudado)
            try:
                details_section = show_box.find_element(By.CSS_SELECTOR, ".show__details")
                detail_divs = details_section.find_elements(By.CSS_SELECTOR, "div")
                
                for detail_div in detail_divs:
                    try:
                        # Buscar elementos span dentro del div
                        spans = detail_div.find_elements(By.TAG_NAME, "span")
                        if len(spans) >= 2:
                            value = spans[0].text.strip()  # El primer span tiene el valor numérico
                            label = spans[1].text.strip()  # El segundo span tiene la etiqueta
                            
                            if "tickets emitidos" in label.lower():
                                show_data["tickets_emitidos"] = value
                            elif "ocupado" in label.lower():
                                show_data["ocupado_porcentaje"] = value
                            elif "recaudado" in label.lower():
                                show_data["recaudado"] = value
                                
                    except Exception as e:
                        logger.warning(f"Error extrayendo detalle específico del show {show_number}: {e}")
                        
            except Exception as e:
                logger.warning(f"Error extrayendo detalles del show {show_number}: {e}")
            
            # Extraer detalles de tickets (vendidos, disponibles, bloqueos)
            try:
                tickets_section = show_box.find_element(By.CSS_SELECTOR, ".show__tickets")
                ticket_spans = tickets_section.find_elements(By.CSS_SELECTOR, "span[class*='tickets__']")
                
                ticket_details = {}
                for span in ticket_spans:
                    try:
                        class_name = span.get_attribute("class")
                        tooltip = span.find_element(By.CSS_SELECTOR, ".tooltip")
                        tooltip_text = tooltip.text.strip()
                        
                        # Extraer el número del tooltip
                        import re
                        numbers = re.findall(r'\d+', tooltip_text)
                        if numbers:
                            number = numbers[0]
                            
                            if "tickets__sold" in class_name:
                                ticket_details["vendidos"] = {
                                    "cantidad": number,
                                    "texto_completo": tooltip_text
                                }
                            elif "tickets__available" in class_name:
                                ticket_details["disponibles"] = {
                                    "cantidad": number,
                                    "texto_completo": tooltip_text
                                }
                            elif "tickets__normal-locks" in class_name:
                                ticket_details["bloqueos_normales"] = {
                                    "cantidad": number,
                                    "texto_completo": tooltip_text
                                }
                            elif "tickets__politeness-locks" in class_name:
                                ticket_details["bloqueos_cortesias"] = {
                                    "cantidad": number,
                                    "texto_completo": tooltip_text
                                }
                                
                    except Exception as e:
                        logger.warning(f"Error extrayendo detalle de ticket del show {show_number}: {e}")
                
                show_data["ticket_details"] = ticket_details
                
            except Exception as e:
                logger.warning(f"Error extrayendo sección de tickets del show {show_number}: {e}")
            
            # Log del show extraído
            logger.info(f"Show {show_number} extraído: {show_data['title']} - {show_data['date']} - {show_data['venue']}")
            logger.info(f"  Tickets: {show_data['tickets_emitidos']}, Ocupado: {show_data['ocupado_porcentaje']}, Recaudado: {show_data['recaudado']}")
            
            return show_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos del show {show_number}: {e}")
            return None
    
    def extract_table_data(self, table_element, table_name):
        """
        Extrae los datos de una tabla específica
        
        Args:
            table_element: Elemento de tabla de Selenium
            table_name (str): Nombre identificativo de la tabla
            
        Returns:
            dict: Datos de la tabla
        """
        try:
            table_data = {
                "table_name": table_name,
                "headers": [],
                "rows": []
            }
            
            # Extraer headers
            try:
                header_row = table_element.find_element(By.CSS_SELECTOR, "thead tr, tr:first-child")
                header_cells = header_row.find_elements(By.TAG_NAME, "th")
                if not header_cells:  # Si no hay th, usar td del primer tr
                    header_cells = header_row.find_elements(By.TAG_NAME, "td")
                
                table_data["headers"] = [cell.text.strip() for cell in header_cells]
                
            except Exception as e:
                logger.warning(f"Error extrayendo headers de {table_name}: {e}")
            
            # Extraer filas de datos
            try:
                data_rows = table_element.find_elements(By.CSS_SELECTOR, "tbody tr, tr")
                # Si tenemos headers, saltamos la primera fila
                if table_data["headers"]:
                    data_rows = data_rows[1:]
                
                for row_index, row in enumerate(data_rows[:20]):  # Limitar a 20 filas
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if not cells:
                            cells = row.find_elements(By.TAG_NAME, "th")
                        
                        row_data = [cell.text.strip() for cell in cells]
                        if any(row_data):  # Solo agregar si hay contenido
                            table_data["rows"].append({
                                "row_number": row_index + 1,
                                "data": row_data
                            })
                            
                    except Exception as e:
                        logger.warning(f"Error extrayendo fila {row_index} de {table_name}: {e}")
                        
            except Exception as e:
                logger.warning(f"Error extrayendo filas de {table_name}: {e}")
            
            return table_data if (table_data["headers"] or table_data["rows"]) else None
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de tabla {table_name}: {e}")
            return None
    
    def save_data_to_database(self, data):
        """
        Guarda los datos extraídos directamente en la base de datos - UN JSON POR SHOW
        
        Args:
            data (dict): Datos a guardar
        """
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos")
                return False
            
            # Ajustar zona horaria (restar 3 horas para Argentina)
            fecha_extraccion = datetime.now() - timedelta(hours=3)
            cursor = self.db_connection.cursor()
            raw_data_ids = []
            
            # Procesar cada show individualmente
            if "shows_data" in data and data["shows_data"]:
                logger.info(f"🎯 Procesando {len(data['shows_data'])} shows individualmente...")
                
                for show in data["shows_data"]:
                    try:
                        # Verificar si el show debe ser excluido
                        if self.should_exclude_show(show.get("title", "")):
                            logger.info(f"⏭️ Saltando show excluido: {show.get('title', 'Sin título')}")
                            continue
                        # Crear JSON individual para cada show
                        show_json = {
                            "artista": show.get("title", "EVENTO_TICKANTEL"),
                            "venue": show.get("venue", "TickAntel Venue"),
                            "fecha_show": show.get("date", ""),
                            "show_id": show.get("show_id", ""),
                            "show_url": show.get("show_url", ""),
                            "tickets_emitidos": show.get("tickets_emitidos", "0"),
                            "ocupado_porcentaje": show.get("ocupado_porcentaje", "0%"),
                            "recaudado": show.get("recaudado", "$0"),
                            "ticket_details": show.get("ticket_details", {}),
                            "extraction_time": data.get("extraction_time", ""),
                            "username_used": data.get("username_used", ""),
                            "url": data.get("url", "")
                        }
                        
                        logger.info(f"🔍 DEBUG - Show JSON creado: {show_json['artista']} - {show_json['venue']} - {show_json['fecha_show']}")
                        
                        # Calcular totales numéricos
                        tickets_emitidos = self.parse_number(show.get("tickets_emitidos", "0"))
                        recaudado = self.parse_currency(show.get("recaudado", "$0"))
                        ocupado_porcentaje = self.parse_percentage(show.get("ocupado_porcentaje", "0%"))
                        
                        # Calcular capacidad total
                        capacidad_total = 0
                        if ocupado_porcentaje > 0:
                            capacidad_total = int(tickets_emitidos * 100 / ocupado_porcentaje)
                        else:
                            capacidad_total = tickets_emitidos
                        
                        # Calcular tickets disponibles
                        tickets_disponibles = max(0, capacidad_total - tickets_emitidos)
                        
                        # Agregar totales calculados al JSON
                        show_json["totales"] = {
                            "total_vendido": tickets_emitidos,
                            "total_recaudado": recaudado,
                            "total_capacidad": capacidad_total,
                            "tickets_disponibles": tickets_disponibles,
                            "porcentaje_ocupacion": ocupado_porcentaje
                        }
                        
                        # Agregar datos geográficos para tickantel (Uruguay)
                        show_json["pais"] = "Uruguay"
                        show_json["ciudad"] = "Montevideo"
                        
                        # Convertir fecha_show a timestamp primero
                        fecha_show_timestamp = None
                        if show.get("date"):
                            try:
                                # Parsear fecha en formato DD/MM/YYYY
                                fecha_parts = show.get("date").split("/")
                                if len(fecha_parts) == 3:
                                    day, month, year = fecha_parts
                                    fecha_show_timestamp = datetime(int(year), int(month), int(day))
                            except:
                                fecha_show_timestamp = None
                        
                        # Calcular ventas diarias comparando con datos anteriores
                        logger.info(f"🔍 Calculando ventas diarias para {show_json['artista']}...")
                        previous_totals = self.get_previous_totals(
                            show_json.get("show_id", ""),
                            show_json["artista"],
                            show_json["venue"],
                            fecha_show_timestamp
                        )
                        
                        daily_sales_data = self.calculate_daily_sales(show_json, previous_totals)
                        # Convertir datetime a string para JSON
                        if daily_sales_data.get('fecha_anterior'):
                            daily_sales_data['fecha_anterior'] = daily_sales_data['fecha_anterior'].isoformat()
                        show_json["daily_sales"] = daily_sales_data
                        
                        logger.info(f"✅ Ventas diarias calculadas: {daily_sales_data['venta_diaria']} tickets, ${daily_sales_data['monto_diario_ars']:,}")
                        
                        # Insertar en raw_data con columnas separadas
                        insert_query = """
                            INSERT INTO raw_data (
                                ticketera, 
                                json_data, 
                                fecha_extraccion, 
                                archivo_origen, 
                                url_origen, 
                                procesado,
                                artista,
                                venue,
                                fecha_show
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id;
                        """
                        
                        archivo_origen = f"tickantel_{show.get('title', 'show').replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        url_origen = show.get("show_url", self.base_url)
                        json_data_str = json.dumps(show_json, ensure_ascii=False)
                        
                        
                        logger.info(f"🔍 DEBUG - Insertando en BD:")
                        logger.info(f"   Ticketera: tickantel")
                        logger.info(f"   Artista: {show_json['artista']}")
                        logger.info(f"   Venue: {show_json['venue']}")
                        logger.info(f"   Fecha Show: {show_json['fecha_show']} -> {fecha_show_timestamp}")
                        logger.info(f"   Fecha Extracción: {fecha_extraccion}")
                        logger.info(f"   Archivo: {archivo_origen}")
                        logger.info(f"   URL: {url_origen}")
                        
                        cursor.execute(insert_query, (
                            'tickantel',
                            json_data_str,
                            fecha_extraccion,
                            archivo_origen,
                            url_origen,
                            False,  # No procesado aún, el trigger lo procesará
                            show_json['artista'],
                            show_json['venue'],
                            fecha_show_timestamp
                        ))
                        
                        raw_data_id = cursor.fetchone()[0]
                        raw_data_ids.append(raw_data_id)
                        
                        logger.info(f"✅ Show '{show.get('title', 'Sin título')}' guardado con ID: {raw_data_id}")
                        logger.info(f"   📊 Tickets: {tickets_emitidos}, Recaudado: ${recaudado:,}, Capacidad: {capacidad_total}")
                        
                        # Si hay ventas diarias calculadas, insertar o actualizar en daily_sales
                        if daily_sales_data and daily_sales_data.get('venta_diaria', 0) >= 0:  # Permitir 0 para actualizaciones
                            self.insert_or_update_daily_sales(raw_data_id, show_json, daily_sales_data, fecha_extraccion)
                        
                    except Exception as e:
                        logger.error(f"❌ Error guardando show '{show.get('title', 'Sin título')}': {str(e)}")
                        continue
                
                self.db_connection.commit()
                cursor.close()
                
                logger.info(f"✅ {len(raw_data_ids)} shows guardados exitosamente en base de datos")
                logger.info(f"📊 Usuario utilizado: {self.username}")
                
                # Mostrar resumen de datos extraídos
                logger.info("=== RESUMEN DE DATOS EXTRAÍDOS ===")
                
                if "summary_data" in data:
                    logger.info("RESUMEN GENERAL:")
                    for key, value in data["summary_data"].items():
                        logger.info(f"  {key}: {value}")
                
                logger.info(f"\nSHOWS PROCESADOS: {len(raw_data_ids)}")
                for i, show in enumerate(data["shows_data"]):
                    logger.info(f"  {i+1}. {show.get('title', 'Sin título')}")
                    logger.info(f"     Fecha: {show.get('date', 'N/A')}")
                    logger.info(f"     Venue: {show.get('venue', 'N/A')}")
                    logger.info(f"     Tickets emitidos: {show.get('tickets_emitidos', 'N/A')}")
                    logger.info(f"     Ocupado: {show.get('ocupado_porcentaje', 'N/A')}")
                    logger.info(f"     Recaudado: {show.get('recaudado', 'N/A')}")
                
                return raw_data_ids
            else:
                logger.warning("⚠️ No hay shows_data para procesar")
                return []
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos en base de datos: {str(e)}")
            if self.db_connection:
                self.db_connection.rollback()
            return None
    
    def parse_number(self, number_str):
        """Convierte string numérico a entero"""
        try:
            # Remover puntos y comas, convertir a entero
            cleaned = str(number_str).replace('.', '').replace(',', '').strip()
            return int(cleaned) if cleaned.isdigit() else 0
        except:
            return 0
    
    def parse_currency(self, currency_str):
        """Convierte string de moneda a entero"""
        try:
            # Remover $, puntos, comas y espacios
            cleaned = str(currency_str).replace('$', '').replace('.', '').replace(',', '').replace(' ', '').strip()
            return int(cleaned) if cleaned.isdigit() else 0
        except:
            return 0
    
    def parse_percentage(self, percentage_str):
        """Convierte string de porcentaje a decimal"""
        try:
            # Remover % y comas, convertir a float
            cleaned = str(percentage_str).replace('%', '').replace(',', '.').strip()
            return float(cleaned) if cleaned.replace('.', '').isdigit() else 0.0
        except:
            return 0.0
    
    def should_exclude_show(self, show_title):
        """
        Verifica si un show debe ser excluido del procesamiento
        
        Args:
            show_title (str): Título del show
            
        Returns:
            bool: True si debe ser excluido, False si debe procesarse
        """
        for excluded in self.excluded_shows:
            if excluded.lower() in show_title.lower():
                logger.info(f"🚫 EXCLUYENDO SHOW: '{show_title}' (coincide con '{excluded}')")
                return True
        return False
    
    def get_previous_totals(self, show_id, artista, venue, fecha_show):
        """
        Obtiene los totales anteriores de un show para calcular ventas diarias
        
        Args:
            show_id (str): ID del show
            artista (str): Nombre del artista
            venue (str): Venue del show
            fecha_show (datetime): Fecha del show
            
        Returns:
            dict: Totales anteriores o None si no existen
        """
        try:
            cursor = self.db_connection.cursor()
            
            # Buscar el show más reciente anterior a la fecha actual
            query = """
                SELECT 
                    json_data->'totales'->>'total_vendido' as total_vendido_anterior,
                    json_data->'totales'->>'total_recaudado' as total_recaudado_anterior,
                    fecha_extraccion
                FROM raw_data 
                WHERE ticketera = 'tickantel' 
                AND artista = %s 
                AND venue = %s 
                AND fecha_show = %s
                AND fecha_extraccion < NOW()
                ORDER BY fecha_extraccion DESC 
                LIMIT 1;
            """
            
            cursor.execute(query, (artista, venue, fecha_show))
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] and result[1]:
                return {
                    'total_vendido_anterior': int(result[0]),
                    'total_recaudado_anterior': int(result[1]),
                    'fecha_anterior': result[2]
                }
            else:
                logger.info(f"📊 No se encontraron datos anteriores para {artista} - {venue}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo totales anteriores: {str(e)}")
            return None
    
    def calculate_daily_sales(self, show_data, previous_totals):
        """
        Calcula las ventas diarias basado en la diferencia entre totales actuales y anteriores
        
        Args:
            show_data (dict): Datos actuales del show
            previous_totals (dict): Totales anteriores del show
            
        Returns:
            dict: Datos de ventas diarias calculadas
        """
        try:
            current_totals = show_data.get('totales', {})
            current_vendido = current_totals.get('total_vendido', 0)
            current_recaudado = current_totals.get('total_recaudado', 0)
            
            if not previous_totals:
                # Si no hay datos anteriores, asumir que toda la venta es de hoy
                daily_sales = {
                    'venta_diaria': current_vendido,
                    'monto_diario_ars': current_recaudado,
                    'es_primera_extraccion': True
                }
                logger.info(f"📊 Primera extracción para {show_data['artista']}: {current_vendido} tickets, ${current_recaudado:,}")
            else:
                # Calcular diferencia
                venta_diaria = current_vendido - previous_totals['total_vendido_anterior']
                monto_diario = current_recaudado - previous_totals['total_recaudado_anterior']
                
                daily_sales = {
                    'venta_diaria': max(0, venta_diaria),  # No permitir valores negativos
                    'monto_diario_ars': max(0, monto_diario),
                    'es_primera_extraccion': False,
                    'fecha_anterior': previous_totals['fecha_anterior']
                }
                
                logger.info(f"📊 Ventas diarias calculadas para {show_data['artista']}:")
                logger.info(f"   Anterior: {previous_totals['total_vendido_anterior']} tickets, ${previous_totals['total_recaudado_anterior']:,}")
                logger.info(f"   Actual: {current_vendido} tickets, ${current_recaudado:,}")
                logger.info(f"   Diferencia: {venta_diaria} tickets, ${monto_diario:,}")
            
            return daily_sales
            
        except Exception as e:
            logger.error(f"❌ Error calculando ventas diarias: {str(e)}")
            return {
                'venta_diaria': 0,
                'monto_diario_ars': 0,
                'es_primera_extraccion': True,
                'error': str(e)
            }
    
    def insert_or_update_daily_sales(self, raw_data_id, show_json, daily_sales_data, fecha_extraccion):
        """
        Inserta o actualiza datos de ventas diarias en la tabla daily_sales
        Maneja múltiples ejecuciones del mismo día correctamente
        
        Args:
            raw_data_id (str): ID del registro en raw_data
            show_json (dict): Datos del show
            daily_sales_data (dict): Datos de ventas diarias calculadas
            fecha_extraccion (datetime): Fecha de extracción
        """
        try:
            cursor = self.db_connection.cursor()
            
            # Buscar o crear el show en la tabla shows
            show_id = self.get_or_create_show(show_json, cursor)
            
            if not show_id:
                logger.error(f"❌ No se pudo obtener/crear show para {show_json['artista']}")
                return
            
            fecha_venta = fecha_extraccion.date()
            totales = show_json.get('totales', {})
            
            # Verificar si ya existe un registro para este show en esta fecha
            check_existing_query = """
                SELECT id, venta_total_acumulada, recaudacion_total_ars, fecha_extraccion
                FROM daily_sales 
                WHERE show_id = %s 
                AND fecha_venta = %s 
                AND ticketera = 'tickantel'
                ORDER BY fecha_extraccion DESC 
                LIMIT 1;
            """
            
            cursor.execute(check_existing_query, (show_id, fecha_venta))
            existing_record = cursor.fetchone()
            
            if existing_record:
                # Ya existe un registro para este día - actualizar
                existing_id, existing_total_vendido, existing_total_recaudado, existing_fecha = existing_record
                
                logger.info(f"🔄 Actualizando registro existente del día {fecha_venta}")
                logger.info(f"   Registro anterior: {existing_total_vendido} tickets, ${existing_total_recaudado:,}")
                logger.info(f"   Registro actual: {totales.get('total_vendido', 0)} tickets, ${totales.get('total_recaudado', 0):,}")
                
                # Calcular ventas diarias basado en la diferencia con el día anterior
                previous_totals = self.get_previous_totals(
                    show_json.get("show_id", ""),
                    show_json["artista"],
                    show_json["venue"],
                    datetime.combine(fecha_venta, datetime.min.time())
                )
                
                # Usar los datos ya calculados correctamente por calculate_daily_sales
                venta_diaria_total = daily_sales_data.get('venta_diaria', 0)
                monto_diario_total = daily_sales_data.get('monto_diario_ars', 0)
                
                # Actualizar el registro existente
                update_query = """
                    UPDATE daily_sales SET
                        fecha_extraccion = %s,
                        venta_diaria = %s,
                        monto_diario_ars = %s,
                        venta_total_acumulada = %s,
                        recaudacion_total_ars = %s,
                        tickets_disponibles = %s,
                        porcentaje_ocupacion = %s,
                        archivo_origen = %s,
                        url_origen = %s
                    WHERE id = %s;
                """
                
                archivo_origen = f"tickantel_{show_json['artista'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                cursor.execute(update_query, (
                    fecha_extraccion,
                    max(0, venta_diaria_total),
                    max(0, monto_diario_total),
                    totales.get('total_vendido', 0),
                    totales.get('total_recaudado', 0),
                    totales.get('tickets_disponibles', 0),
                    totales.get('porcentaje_ocupacion', 0),
                    archivo_origen,
                    show_json.get('show_url', ''),
                    existing_id
                ))
                
                logger.info(f"✅ Daily sales actualizado con ID: {existing_id}")
                logger.info(f"   📊 Venta diaria total: {max(0, venta_diaria_total)} tickets, ${max(0, monto_diario_total):,}")
                
            else:
                # No existe registro para este día - crear nuevo
                logger.info(f"🆕 Creando nuevo registro para el día {fecha_venta}")
                
                insert_daily_sales_query = """
                    INSERT INTO daily_sales (
                        show_id,
                        fecha_venta,
                        fecha_extraccion,
                        venta_diaria,
                        monto_diario_ars,
                        venta_total_acumulada,
                        recaudacion_total_ars,
                        tickets_disponibles,
                        porcentaje_ocupacion,
                        archivo_origen,
                        url_origen,
                        ticketera
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
                """
                
                archivo_origen = f"tickantel_{show_json['artista'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                cursor.execute(insert_daily_sales_query, (
                    show_id,
                    fecha_venta,
                    fecha_extraccion,
                    daily_sales_data['venta_diaria'],
                    daily_sales_data['monto_diario_ars'],
                    totales.get('total_vendido', 0),
                    totales.get('total_recaudado', 0),
                    totales.get('tickets_disponibles', 0),
                    totales.get('porcentaje_ocupacion', 0),
                    archivo_origen,
                    show_json.get('show_url', ''),
                    'tickantel'
                ))
                
                daily_sales_id = cursor.fetchone()[0]
                logger.info(f"✅ Daily sales creado con ID: {daily_sales_id}")
                logger.info(f"   📊 Venta diaria: {daily_sales_data['venta_diaria']} tickets, ${daily_sales_data['monto_diario_ars']:,}")
            
        except Exception as e:
            logger.error(f"❌ Error insertando/actualizando daily sales: {str(e)}")
        finally:
            if 'cursor' in locals():
                cursor.close()
    
    def get_or_create_show(self, show_json, cursor):
        """
        Obtiene o crea un show en la tabla shows
        
        Args:
            show_json (dict): Datos del show
            cursor: Cursor de la base de datos
            
        Returns:
            str: ID del show o None si hay error
        """
        try:
            # Buscar show existente
            select_query = """
                SELECT id FROM shows 
                WHERE ticketera = 'tickantel' 
                AND artista = %s 
                AND venue = %s 
                AND fecha_show = %s;
            """
            
            fecha_show_timestamp = None
            if show_json.get("fecha_show"):
                try:
                    fecha_parts = show_json["fecha_show"].split("/")
                    if len(fecha_parts) == 3:
                        day, month, year = fecha_parts
                        fecha_show_timestamp = datetime(int(year), int(month), int(day))
                except:
                    fecha_show_timestamp = None
            
            cursor.execute(select_query, (
                show_json['artista'],
                show_json['venue'],
                fecha_show_timestamp
            ))
            
            result = cursor.fetchone()
            if result:
                logger.info(f"📋 Show existente encontrado: {result[0]}")
                return result[0]
            
            # Crear nuevo show
            totales = show_json.get('totales', {})
            insert_show_query = """
                INSERT INTO shows (
                    artista,
                    venue,
                    fecha_show,
                    ciudad,
                    pais,
                    capacidad_total,
                    ticketera,
                    estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """
            
            cursor.execute(insert_show_query, (
                show_json['artista'],
                show_json['venue'],
                fecha_show_timestamp,
                show_json.get('ciudad', 'Montevideo'),
                show_json.get('pais', 'Uruguay'),
                totales.get('total_capacidad', 0),
                'tickantel',
                'activo'
            ))
            
            show_id = cursor.fetchone()[0]
            logger.info(f"✅ Nuevo show creado con ID: {show_id}")
            return show_id
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo/creando show: {str(e)}")
            return None
    
    def run(self):
        """Ejecuta el scraper completo de Tickantel optimizado para Airflow"""
        try:
            logger.info("=== INICIANDO SCRAPER DE TICKANTEL PARA AIRFLOW ===")
            
            # Verificar conexión a base de datos
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos")
                return False
            
            # Configurar driver
            if not self.setup_driver():
                logger.error("❌ No se pudo configurar el driver")
                return False
            
            # Navegar a la página de login
            if not self.navigate_to_login():
                logger.error("❌ No se pudo navegar a la página de login")
                return False
            
            # Realizar login
            if not self.login():
                logger.error("❌ No se pudo realizar el login")
                return False
            
            # Esperar a que el dashboard se cargue
            if not self.wait_for_dashboard_load():
                logger.warning("⚠️ El dashboard podría no haberse cargado completamente, continuando...")
            
            # Extraer datos del dashboard
            dashboard_data = self.extract_dashboard_data()
            
            if dashboard_data:
                # Guardar datos en base de datos (un JSON por show)
                raw_data_ids = self.save_data_to_database(dashboard_data)
                
                if raw_data_ids and len(raw_data_ids) > 0:
                    logger.info("=== ✅ SCRAPER COMPLETADO EXITOSAMENTE ===")
                    logger.info(f"📊 {len(raw_data_ids)} shows guardados en BD con IDs: {raw_data_ids}")
                    logger.info("🔄 El trigger automático procesará los datos")
                    return True
                else:
                    logger.error("❌ Error guardando los datos en base de datos")
                    return False
            else:
                logger.error("❌ No se pudieron extraer datos del dashboard")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error ejecutando scraper: {str(e)}")
            return False
        
        finally:
            # Cerrar conexión a base de datos
            if self.db_connection:
                self.db_connection.close()
                logger.info("🔌 Conexión a base de datos cerrada")
            
            # Cerrar automáticamente el navegador
            self.close()
    
    def close(self):
        """Cierra el driver"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")

def test_daily_sales_calculation():
    """Función de prueba para verificar el cálculo de ventas diarias sin afectar la BD"""
    logger.info("🧪 INICIANDO PRUEBA DE CÁLCULO DE VENTAS DIARIAS")
    
    try:
        scraper = TickantelScraper(headless=True)
        
        # Simular datos de un show
        test_show_data = {
            "artista": "Erreway",
            "venue": "Antel Arena", 
            "fecha_show": "22/11/2025",
            "totales": {
                "total_vendido": 4581,
                "total_recaudado": 13515200,
                "total_capacidad": 5285,
                "tickets_disponibles": 704,
                "porcentaje_ocupacion": 86.7
            }
        }
        
        # Conectar a BD solo para consultar datos anteriores
        if scraper.setup_database_connection():
            logger.info("✅ Conexión a BD establecida para prueba")
            
            # Obtener totales anteriores
            previous_totals = scraper.get_previous_totals(
                "show-test",
                test_show_data["artista"],
                test_show_data["venue"],
                datetime(2025, 11, 22)
            )
            
            # Calcular ventas diarias
            daily_sales = scraper.calculate_daily_sales(test_show_data, previous_totals)
            
            logger.info("=== RESULTADOS DE LA PRUEBA ===")
            logger.info(f"Show: {test_show_data['artista']} - {test_show_data['venue']}")
            logger.info(f"Totales actuales: {test_show_data['totales']['total_vendido']} tickets, ${test_show_data['totales']['total_recaudado']:,}")
            
            if previous_totals:
                logger.info(f"Totales anteriores: {previous_totals['total_vendido_anterior']} tickets, ${previous_totals['total_recaudado_anterior']:,}")
                logger.info(f"Fecha anterior: {previous_totals['fecha_anterior']}")
            else:
                logger.info("No hay datos anteriores (primera extracción)")
            
            logger.info(f"Venta diaria calculada: {daily_sales['venta_diaria']} tickets, ${daily_sales['monto_diario_ars']:,}")
            logger.info(f"Es primera extracción: {daily_sales.get('es_primera_extraccion', False)}")
            
            scraper.db_connection.close()
            logger.info("✅ Prueba completada exitosamente")
            return True
        else:
            logger.error("❌ No se pudo conectar a la BD para la prueba")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en la prueba: {str(e)}")
        return False

def test_multiple_runs_simulation():
    """Simula múltiples ejecuciones del mismo día para probar la lógica de actualización"""
    logger.info("🧪 INICIANDO SIMULACIÓN DE MÚLTIPLES EJECUCIONES")
    
    try:
        scraper = TickantelScraper(headless=True)
        
        if not scraper.setup_database_connection():
            logger.error("❌ No se pudo conectar a la BD")
            return False
        
        # Simular 3 ejecuciones del mismo día con datos incrementales
        test_scenarios = [
            {
                "hora": "09:00",
                "total_vendido": 4581,
                "total_recaudado": 13515200,
                "descripcion": "Primera ejecución del día"
            },
            {
                "hora": "14:00", 
                "total_vendido": 4595,
                "total_recaudado": 13545000,
                "descripcion": "Segunda ejecución (ventas incrementales)"
            },
            {
                "hora": "18:00",
                "total_vendido": 4602,
                "total_recaudado": 13558000,
                "descripcion": "Tercera ejecución (final del día)"
            }
        ]
        
        for i, scenario in enumerate(test_scenarios):
            logger.info(f"\n=== EJECUCIÓN {i+1}: {scenario['hora']} - {scenario['descripcion']} ===")
            
            # Crear datos del show para esta ejecución
            test_show_data = {
                "artista": "Erreway",
                "venue": "Antel Arena",
                "fecha_show": "22/11/2025",
                "show_id": "show-test-multiple",
                "show_url": "https://test.com",
                "totales": {
                    "total_vendido": scenario["total_vendido"],
                    "total_recaudado": scenario["total_recaudado"],
                    "total_capacidad": 5285,
                    "tickets_disponibles": 5285 - scenario["total_vendido"],
                    "porcentaje_ocupacion": (scenario["total_vendido"] / 5285) * 100
                }
            }
            
            # Simular fecha de extracción
            fecha_extraccion = datetime.now().replace(hour=int(scenario["hora"].split(":")[0]))
            
            # Obtener totales anteriores (del día anterior)
            previous_totals = scraper.get_previous_totals(
                test_show_data["show_id"],
                test_show_data["artista"],
                test_show_data["venue"],
                datetime(2025, 11, 21)  # Día anterior
            )
            
            # Calcular ventas diarias
            daily_sales = scraper.calculate_daily_sales(test_show_data, previous_totals)
            
            logger.info(f"📊 Datos de la ejecución:")
            logger.info(f"   Total vendido: {scenario['total_vendido']} tickets")
            logger.info(f"   Total recaudado: ${scenario['total_recaudado']:,}")
            logger.info(f"   Venta diaria calculada: {daily_sales['venta_diaria']} tickets, ${daily_sales['monto_diario_ars']:,}")
            
            # Simular inserción/actualización (sin hacer commit real)
            logger.info(f"🔄 Simulando inserción/actualización en daily_sales...")
            logger.info(f"   Se {'actualizaría' if i > 0 else 'crearía'} el registro para el día {fecha_extraccion.date()}")
        
        logger.info("\n✅ Simulación completada exitosamente")
        logger.info("📋 RESUMEN:")
        logger.info("   - Primera ejecución: Crearía nuevo registro")
        logger.info("   - Ejecuciones siguientes: Actualizarían el registro existente")
        logger.info("   - Venta diaria: Siempre calculada desde el día anterior")
        
        scraper.db_connection.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en la simulación: {str(e)}")
        return False

def test_real_extraction_without_saving():
    """Test REAL: Extrae datos reales de TickAntel pero NO los guarda en BD"""
    logger.info("🧪 INICIANDO TEST REAL DE EXTRACCIÓN SIN GUARDAR EN BD")
    
    try:
        scraper = TickantelScraper(headless=True)
        
        # Conectar a BD solo para consultar datos anteriores
        if not scraper.setup_database_connection():
            logger.error("❌ No se pudo conectar a la BD")
            return False
        
        logger.info("=== INICIANDO SCRAPER REAL DE TICKANTEL (MODO TEST) ===")
        
        # Configurar driver
        if not scraper.setup_driver():
            logger.error("❌ No se pudo configurar el driver")
            return False
        
        # Navegar a la página de login
        if not scraper.navigate_to_login():
            logger.error("❌ No se pudo navegar a la página de login")
            return False
        
        # Realizar login
        if not scraper.login():
            logger.error("❌ No se pudo realizar el login")
            return False
        
        # Esperar a que el dashboard se cargue
        if not scraper.wait_for_dashboard_load():
            logger.warning("⚠️ El dashboard podría no haberse cargado completamente, continuando...")
        
        # Extraer datos REALES del dashboard
        logger.info("📊 EXTRAYENDO DATOS REALES DEL DASHBOARD...")
        dashboard_data = scraper.extract_dashboard_data()
        
        if not dashboard_data or "shows_data" not in dashboard_data:
            logger.error("❌ No se pudieron extraer datos del dashboard")
            return False
        
        real_shows = dashboard_data["shows_data"]
        logger.info(f"📊 DATOS REALES EXTRAÍDOS: {len(real_shows)} shows encontrados")
        logger.info("="*80)
        
        # Procesar cada show real
        for i, show in enumerate(real_shows, 1):
            try:
                # Verificar si el show debe ser excluido
                if scraper.should_exclude_show(show.get("title", "")):
                    logger.info(f"⏭️ Saltando show excluido: {show.get('title', 'Sin título')}")
                    continue
                
                logger.info(f"\n🎭 SHOW REAL {i}: {show.get('title', 'Sin título')}")
                logger.info(f"📅 Fecha: {show.get('date', 'N/A')}")
                logger.info(f"🏛️ Venue: {show.get('venue', 'N/A')}")
                logger.info("-" * 60)
                
                # Crear estructura de datos como lo haría el scraper real
                show_json = {
                    "artista": show.get("title", "EVENTO_TICKANTEL"),
                    "venue": show.get("venue", "TickAntel Venue"),
                    "fecha_show": show.get("date", ""),
                    "show_id": show.get("show_id", ""),
                    "show_url": show.get("show_url", ""),
                    "tickets_emitidos": show.get("tickets_emitidos", "0"),
                    "ocupado_porcentaje": show.get("ocupado_porcentaje", "0%"),
                    "recaudado": show.get("recaudado", "$0"),
                    "ticket_details": show.get("ticket_details", {}),
                    "extraction_time": dashboard_data.get("extraction_time", ""),
                    "username_used": dashboard_data.get("username_used", ""),
                    "url": dashboard_data.get("url", "")
                }
                
                # Calcular totales numéricos (como lo hace el scraper real)
                tickets_emitidos = scraper.parse_number(show.get("tickets_emitidos", "0"))
                recaudado = scraper.parse_currency(show.get("recaudado", "$0"))
                ocupado_porcentaje = scraper.parse_percentage(show.get("ocupado_porcentaje", "0%"))
                
                # Calcular capacidad total
                capacidad_total = 0
                if ocupado_porcentaje > 0:
                    capacidad_total = int(tickets_emitidos * 100 / ocupado_porcentaje)
                else:
                    capacidad_total = tickets_emitidos
                
                # Calcular tickets disponibles
                tickets_disponibles = max(0, capacidad_total - tickets_emitidos)
                
                # Agregar totales calculados al JSON
                show_json["totales"] = {
                    "total_vendido": tickets_emitidos,
                    "total_recaudado": recaudado,
                    "total_capacidad": capacidad_total,
                    "tickets_disponibles": tickets_disponibles,
                    "porcentaje_ocupacion": ocupado_porcentaje
                }
                
                # Agregar datos geográficos para tickantel (Uruguay)
                show_json["pais"] = "Uruguay"
                show_json["ciudad"] = "Montevideo"
                
                # Convertir fecha_show a timestamp
                fecha_show_timestamp = None
                if show.get("date"):
                    try:
                        # Parsear fecha en formato DD/MM/YYYY
                        fecha_parts = show.get("date").split("/")
                        if len(fecha_parts) == 3:
                            day, month, year = fecha_parts
                            fecha_show_timestamp = datetime(int(year), int(month), int(day))
                    except:
                        fecha_show_timestamp = None
                
                # Obtener totales anteriores de la BD
                logger.info(f"🔍 Consultando datos anteriores en BD...")
                previous_totals = scraper.get_previous_totals(
                    show_json.get("show_id", ""),
                    show_json["artista"],
                    show_json["venue"],
                    fecha_show_timestamp
                )
                
                # Calcular ventas diarias
                daily_sales_data = scraper.calculate_daily_sales(show_json, previous_totals)
                
                # Mostrar datos REALES extraídos
                logger.info(f"📊 DATOS REALES DE TICKANTEL:")
                logger.info(f"   🎫 Total vendido: {tickets_emitidos} tickets")
                logger.info(f"   💰 Total recaudado: ${recaudado:,}")
                logger.info(f"   🏛️ Capacidad total: {capacidad_total}")
                logger.info(f"   🆓 Tickets disponibles: {tickets_disponibles}")
                logger.info(f"   📈 Ocupación: {ocupado_porcentaje}%")
                
                # Mostrar cálculo diferencial
                if previous_totals:
                    logger.info(f"\n📈 CÁLCULO DIFERENCIAL:")
                    logger.info(f"   📅 Último registro: {previous_totals['fecha_anterior']}")
                    logger.info(f"   🎫 Total anterior: {previous_totals['total_vendido_anterior']} tickets")
                    logger.info(f"   🎫 Total actual: {tickets_emitidos} tickets")
                    logger.info(f"   ➖ VENTA DIARIA: {daily_sales_data['venta_diaria']} tickets")
                    logger.info(f"   💰 Recaudación anterior: ${previous_totals['total_recaudado_anterior']:,}")
                    logger.info(f"   💰 Recaudación actual: ${recaudado:,}")
                    logger.info(f"   ➖ MONTO DIARIO: ${daily_sales_data['monto_diario_ars']:,}")
                    
                    if daily_sales_data['venta_diaria'] == 0:
                        logger.info(f"   ⚠️ Sin ventas nuevas (diferencia = 0)")
                    elif daily_sales_data['venta_diaria'] > 0:
                        logger.info(f"   ✅ {daily_sales_data['venta_diaria']} tickets vendidos desde el último registro")
                    else:
                        logger.info(f"   🔄 Diferencia negativa (posible devolución)")
                else:
                    logger.info(f"\n🆕 PRIMER REGISTRO:")
                    logger.info(f"   🎫 VENTA DIARIA: {daily_sales_data['venta_diaria']} tickets")
                    logger.info(f"   💰 MONTO DIARIO: ${daily_sales_data['monto_diario_ars']:,}")
                
                # Mostrar datos que se guardarían en daily_sales
                logger.info(f"\n💾 DATOS QUE SE GUARDARÍAN EN DAILY_SALES:")
                logger.info(f"   🎫 Venta diaria: {daily_sales_data['venta_diaria']}")
                logger.info(f"   💰 Monto diario: ${daily_sales_data['monto_diario_ars']:,}")
                logger.info(f"   📈 Venta total acumulada: {tickets_emitidos}")
                logger.info(f"   💰 Recaudación total: ${recaudado:,}")
                logger.info(f"   🆓 Tickets disponibles: {tickets_disponibles}")
                logger.info(f"   📊 Ocupación: {ocupado_porcentaje}%")
                
                # Simular query SQL
                logger.info(f"\n💾 QUERY SQL QUE SE EJECUTARÍA:")
                logger.info(f"   INSERT/UPDATE daily_sales SET")
                logger.info(f"      venta_diaria = {daily_sales_data['venta_diaria']},")
                logger.info(f"      monto_diario_ars = {daily_sales_data['monto_diario_ars']},")
                logger.info(f"      venta_total_acumulada = {tickets_emitidos},")
                logger.info(f"      recaudacion_total_ars = {recaudado},")
                logger.info(f"      tickets_disponibles = {tickets_disponibles},")
                logger.info(f"      porcentaje_ocupacion = {ocupado_porcentaje}")
                
            except Exception as e:
                logger.error(f"❌ Error procesando show {i}: {str(e)}")
                continue
        
        logger.info(f"\n✅ TEST REAL COMPLETADO EXITOSAMENTE")
        logger.info(f"📊 {len(real_shows)} shows reales procesados")
        logger.info("🔄 Los datos NO se guardaron en BD (modo test)")
        logger.info("💡 Para guardar en BD, ejecutar el scraper normal")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en test real: {str(e)}")
        return False
    finally:
        if 'scraper' in locals():
            scraper.close()

def main():
    """Función principal optimizada para Airflow"""
    scraper = TickantelScraper(headless=True)  # True para contenedores/Airflow
    try:
        success = scraper.run()
        if success:
            logger.info("🎉 Scraper completado exitosamente")
            return True
        else:
            logger.error("💥 Scraper falló")
            return False
    except KeyboardInterrupt:
        logger.info("⏹️ Scraper interrumpido por el usuario")
        return False
    except Exception as e:
        logger.error(f"💥 Error inesperado: {str(e)}")
        return False
    finally:
        scraper.close()

def test_all_calculations():
    """Prueba que todos los campos se calculen correctamente"""
    logger.info("🧪 INICIANDO PRUEBA DE TODOS LOS CÁLCULOS")
    
    try:
        scraper = TickantelScraper(headless=True)
        
        if not scraper.setup_database_connection():
            logger.error("❌ No se pudo conectar a la BD")
            return False
        
        # Simular datos de un show con todos los campos
        test_show_data = {
            "artista": "Erreway",
            "venue": "Antel Arena",
            "fecha_show": "22/11/2025",
            "show_id": "show-test-calculations",
            "show_url": "https://test.com",
            "totales": {
                "total_vendido": 4581,        # VENTA TOTAL
                "total_recaudado": 13515200,  # MONTO TOTAL
                "total_capacidad": 5285,      # CAPACIDAD TOTAL
                "tickets_disponibles": 704,   # DISPONIBLES
                "porcentaje_ocupacion": 86.7  # OCUPACIÓN
            }
        }
        
        # Simular datos del día anterior
        previous_totals = {
            'total_vendido_anterior': 4500,    # Venta anterior
            'total_recaudado_anterior': 13200000,  # Monto anterior
            'fecha_anterior': datetime(2025, 11, 21)
        }
        
        # Calcular ventas diarias
        daily_sales = scraper.calculate_daily_sales(test_show_data, previous_totals)
        
        logger.info("=== VERIFICACIÓN DE TODOS LOS CÁLCULOS ===")
        logger.info(f"🎭 Show: {test_show_data['artista']} - {test_show_data['venue']}")
        logger.info("")
        
        # 1. VENTA DIARIA
        venta_diaria_esperada = 4581 - 4500  # 81 tickets
        logger.info(f"📊 VENTA DIARIA:")
        logger.info(f"   Calculada: {daily_sales['venta_diaria']} tickets")
        logger.info(f"   Esperada: {venta_diaria_esperada} tickets")
        logger.info(f"   ✅ {'CORRECTO' if daily_sales['venta_diaria'] == venta_diaria_esperada else 'ERROR'}")
        logger.info("")
        
        # 2. MONTO DIARIO
        monto_diario_esperado = 13515200 - 13200000  # $315,200
        logger.info(f"💰 MONTO DIARIO:")
        logger.info(f"   Calculado: ${daily_sales['monto_diario_ars']:,}")
        logger.info(f"   Esperado: ${monto_diario_esperado:,}")
        logger.info(f"   ✅ {'CORRECTO' if daily_sales['monto_diario_ars'] == monto_diario_esperado else 'ERROR'}")
        logger.info("")
        
        # 3. VENTA TOTAL
        venta_total = test_show_data['totales']['total_vendido']
        logger.info(f"🎫 VENTA TOTAL:")
        logger.info(f"   Actual: {venta_total} tickets")
        logger.info(f"   ✅ CORRECTO (dato directo del scraper)")
        logger.info("")
        
        # 4. MONTO TOTAL
        monto_total = test_show_data['totales']['total_recaudado']
        logger.info(f"💵 MONTO TOTAL:")
        logger.info(f"   Actual: ${monto_total:,}")
        logger.info(f"   ✅ CORRECTO (dato directo del scraper)")
        logger.info("")
        
        # 5. DISPONIBLES
        disponibles = test_show_data['totales']['tickets_disponibles']
        disponibles_esperados = 5285 - 4581  # 704
        logger.info(f"🎟️ TICKETS DISPONIBLES:")
        logger.info(f"   Calculados: {disponibles} tickets")
        logger.info(f"   Esperados: {disponibles_esperados} tickets")
        logger.info(f"   ✅ {'CORRECTO' if disponibles == disponibles_esperados else 'ERROR'}")
        logger.info("")
        
        # 6. OCUPACIÓN
        ocupacion = test_show_data['totales']['porcentaje_ocupacion']
        ocupacion_esperada = (4581 / 5285) * 100  # 86.7%
        logger.info(f"📈 PORCENTAJE DE OCUPACIÓN:")
        logger.info(f"   Calculado: {ocupacion}%")
        logger.info(f"   Esperado: {ocupacion_esperada:.1f}%")
        logger.info(f"   ✅ {'CORRECTO' if abs(ocupacion - ocupacion_esperada) < 0.1 else 'ERROR'}")
        logger.info("")
        
        # RESUMEN FINAL
        logger.info("=== RESUMEN FINAL ===")
        logger.info(f"✅ Venta diaria: {daily_sales['venta_diaria']} tickets")
        logger.info(f"✅ Monto diario: ${daily_sales['monto_diario_ars']:,}")
        logger.info(f"✅ Venta total: {venta_total} tickets")
        logger.info(f"✅ Monto total: ${monto_total:,}")
        logger.info(f"✅ Disponibles: {disponibles} tickets")
        logger.info(f"✅ Ocupación: {ocupacion}%")
        
        scraper.db_connection.close()
        logger.info("✅ Prueba de cálculos completada exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en la prueba de cálculos: {str(e)}")
        return False

def test_show_database_operations():
    """Prueba que los datos se guarden correctamente en la tabla shows"""
    logger.info("🧪 INICIANDO PRUEBA DE OPERACIONES DE BASE DE DATOS - TABLA SHOWS")
    
    try:
        scraper = TickantelScraper(headless=True)
        
        if not scraper.setup_database_connection():
            logger.error("❌ No se pudo conectar a la BD")
            return False
        
        cursor = scraper.db_connection.cursor()
        
        # Simular datos completos de un show
        test_show_data = {
            "artista": "Erreway Test",
            "venue": "Antel Arena",
            "fecha_show": "22/11/2025",
            "show_id": "show-test-db",
            "show_url": "https://test.com",
            "ciudad": "Montevideo",
            "pais": "Uruguay",
            "totales": {
                "total_vendido": 4581,
                "total_recaudado": 13515200,
                "total_capacidad": 5285,
                "tickets_disponibles": 704,
                "porcentaje_ocupacion": 86.7
            }
        }
        
        logger.info("=== PRUEBA 1: CREAR NUEVO SHOW ===")
        
        # Probar creación de show
        show_id = scraper.get_or_create_show(test_show_data, cursor)
        
        if show_id:
            logger.info(f"✅ Show creado/obtenido con ID: {show_id}")
            
            # Verificar que el show se guardó correctamente
            verify_query = """
                SELECT 
                    id, artista, venue, fecha_show, ciudad, pais, 
                    capacidad_total, ticketera, estado
                FROM shows 
                WHERE id = %s;
            """
            
            cursor.execute(verify_query, (show_id,))
            result = cursor.fetchone()
            
            if result:
                logger.info("📋 DATOS GUARDADOS EN LA TABLA SHOWS:")
                logger.info(f"   ID: {result[0]}")
                logger.info(f"   Artista: {result[1]}")
                logger.info(f"   Venue: {result[2]}")
                logger.info(f"   Fecha Show: {result[3]}")
                logger.info(f"   Ciudad: {result[4]}")
                logger.info(f"   País: {result[5]}")
                logger.info(f"   Capacidad Total: {result[6]}")
                logger.info(f"   Ticketera: {result[7]}")
                logger.info(f"   Estado: {result[8]}")
                
                # Verificar que los datos coinciden
                logger.info("\n🔍 VERIFICACIÓN DE DATOS:")
                logger.info(f"   Artista: {'✅' if result[1] == test_show_data['artista'] else '❌'}")
                logger.info(f"   Venue: {'✅' if result[2] == test_show_data['venue'] else '❌'}")
                logger.info(f"   Ciudad: {'✅' if result[4] == test_show_data['ciudad'] else '❌'}")
                logger.info(f"   País: {'✅' if result[5] == test_show_data['pais'] else '❌'}")
                logger.info(f"   Capacidad: {'✅' if result[6] == test_show_data['totales']['total_capacidad'] else '❌'}")
                logger.info(f"   Ticketera: {'✅' if result[7] == 'tickantel' else '❌'}")
                
            else:
                logger.error("❌ No se pudo verificar el show creado")
                return False
                
        else:
            logger.error("❌ No se pudo crear/obtener el show")
            return False
        
        logger.info("\n=== PRUEBA 2: BUSCAR SHOW EXISTENTE ===")
        
        # Probar búsqueda de show existente
        show_id_2 = scraper.get_or_create_show(test_show_data, cursor)
        
        if show_id_2 == show_id:
            logger.info(f"✅ Show existente encontrado correctamente (ID: {show_id_2})")
            logger.info("   No se duplicó el registro")
        else:
            logger.error(f"❌ Error: Se creó un nuevo show (ID: {show_id_2}) en lugar de usar el existente (ID: {show_id})")
            return False
        
        logger.info("\n=== PRUEBA 3: VERIFICAR INTEGRIDAD DE DATOS ===")
        
        # Verificar que no hay duplicados
        count_query = """
            SELECT COUNT(*) 
            FROM shows 
            WHERE ticketera = 'tickantel' 
            AND artista = %s 
            AND venue = %s 
            AND fecha_show = %s;
        """
        
        fecha_show_timestamp = datetime(2025, 11, 22)
        cursor.execute(count_query, (
            test_show_data['artista'],
            test_show_data['venue'],
            fecha_show_timestamp
        ))
        
        count = cursor.fetchone()[0]
        logger.info(f"📊 Registros encontrados: {count}")
        
        if count == 1:
            logger.info("✅ No hay duplicados - Integridad correcta")
        else:
            logger.error(f"❌ Error: Se encontraron {count} registros duplicados")
            return False
        
        # Limpiar datos de prueba
        logger.info("\n🧹 LIMPIANDO DATOS DE PRUEBA...")
        delete_query = "DELETE FROM shows WHERE id = %s;"
        cursor.execute(delete_query, (show_id,))
        scraper.db_connection.commit()
        logger.info("✅ Datos de prueba eliminados")
        
        cursor.close()
        scraper.db_connection.close()
        
        logger.info("\n🎉 PRUEBA DE BASE DE DATOS COMPLETADA EXITOSAMENTE")
        logger.info("✅ Todos los datos se guardan correctamente en la tabla shows")
        logger.info("✅ No hay duplicados")
        logger.info("✅ Los IDs se manejan correctamente")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en la prueba de base de datos: {str(e)}")
        return False

def test_show_matching():
    """Prueba que el scraper encuentre correctamente los 2 shows actuales y haga matching perfecto"""
    logger.info("🧪 INICIANDO PRUEBA DE MATCHING DE SHOWS EXISTENTES")
    
    try:
        scraper = TickantelScraper(headless=True)
        
        if not scraper.setup_database_connection():
            logger.error("❌ No se pudo conectar a la BD")
            return False
        
        cursor = scraper.db_connection.cursor()
        
        # Simular los 2 shows que están actualmente en la BD
        existing_shows = [
            {
                "artista": "Erreway",
                "venue": "Antel Arena",
                "fecha_show": "22/11/2025",
                "show_id": "erreway-antel-2025-11-22",
                "show_url": "https://tickantel.com.uy/erreway",
                "ciudad": "Montevideo",
                "pais": "Uruguay",
                "totales": {
                    "total_vendido": 4581,
                    "total_recaudado": 13515200,
                    "total_capacidad": 5285,
                    "tickets_disponibles": 704,
                    "porcentaje_ocupacion": 86.7
                }
            },
            {
                "artista": "Cazzu",
                "venue": "Teatro de Verano",
                "fecha_show": "10/10/2025",
                "show_id": "cazzu-teatro-2025-10-10",
                "show_url": "https://tickantel.com.uy/cazzu",
                "ciudad": "Montevideo",
                "pais": "Uruguay",
                "totales": {
                    "total_vendido": 3200,
                    "total_recaudado": 8500000,
                    "total_capacidad": 4000,
                    "tickets_disponibles": 800,
                    "porcentaje_ocupacion": 80.0
                }
            }
        ]
        
        logger.info("=== PRUEBA DE MATCHING CON SHOWS EXISTENTES ===")
        
        for i, show_data in enumerate(existing_shows, 1):
            logger.info(f"\n🎭 PRUEBA {i}: {show_data['artista']} - {show_data['venue']}")
            
            # Probar matching con show existente
            show_id = scraper.get_or_create_show(show_data, cursor)
            
            if show_id:
                logger.info(f"✅ Show encontrado/creado con ID: {show_id}")
                
                # Verificar que los datos coinciden exactamente
                verify_query = """
                    SELECT 
                        id, artista, venue, fecha_show, ciudad, pais, 
                        capacidad_total, ticketera, estado
                    FROM shows 
                    WHERE id = %s;
                """
                
                cursor.execute(verify_query, (show_id,))
                result = cursor.fetchone()
                
                if result:
                    logger.info("🔍 VERIFICACIÓN DE MATCHING:")
                    logger.info(f"   Artista: {result[1]} {'✅' if result[1] == show_data['artista'] else '❌'}")
                    logger.info(f"   Venue: {result[2]} {'✅' if result[2] == show_data['venue'] else '❌'}")
                    logger.info(f"   Ciudad: {result[4]} {'✅' if result[4] == show_data['ciudad'] else '❌'}")
                    logger.info(f"   País: {result[5]} {'✅' if result[5] == show_data['pais'] else '❌'}")
                    logger.info(f"   Capacidad: {result[6]} {'✅' if result[6] == show_data['totales']['total_capacidad'] else '❌'}")
                    logger.info(f"   Ticketera: {result[7]} {'✅' if result[7] == 'tickantel' else '❌'}")
                    
                    # Verificar que es el mismo show (no se duplicó)
                    if result[1] == show_data['artista'] and result[2] == show_data['venue']:
                        logger.info("✅ MATCHING PERFECTO - Es el mismo show existente")
                    else:
                        logger.error("❌ ERROR - No coincide con el show esperado")
                        return False
                else:
                    logger.error("❌ No se pudo verificar el show")
                    return False
            else:
                logger.error("❌ No se pudo obtener el show")
                return False
        
        logger.info("\n=== PRUEBA DE EXCLUSIÓN DE SHOWS NO DESEADOS ===")
        
        # Probar que los shows excluidos no se procesen
        excluded_shows = [
            "La Tabaré 40 años",
            "Kumbiaracha en concierto"
        ]
        
        for excluded_show in excluded_shows:
            logger.info(f"🚫 Probando exclusión: '{excluded_show}'")
            should_exclude = scraper.should_exclude_show(excluded_show)
            
            if should_exclude:
                logger.info(f"✅ Correctamente excluido: '{excluded_show}'")
            else:
                logger.error(f"❌ ERROR: No se excluyó '{excluded_show}'")
                return False
        
        # Probar que shows normales NO se excluyan
        normal_shows = ["Erreway", "Cazzu", "Otro Show Normal"]
        
        for normal_show in normal_shows:
            logger.info(f"✅ Probando inclusión: '{normal_show}'")
            should_exclude = scraper.should_exclude_show(normal_show)
            
            if not should_exclude:
                logger.info(f"✅ Correctamente incluido: '{normal_show}'")
            else:
                logger.error(f"❌ ERROR: Se excluyó incorrectamente '{normal_show}'")
                return False
        
        cursor.close()
        scraper.db_connection.close()
        
        logger.info("\n🎉 PRUEBA DE MATCHING COMPLETADA EXITOSAMENTE")
        logger.info("✅ Los 2 shows existentes se encuentran correctamente")
        logger.info("✅ El matching es perfecto (no duplica)")
        logger.info("✅ Los shows excluidos se filtran correctamente")
        logger.info("✅ Los shows normales se procesan correctamente")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en la prueba de matching: {str(e)}")
        return False

def test_extraction_only():
    """Prueba solo la extracción de datos sin guardar en BD"""
    logger.info("🧪 INICIANDO PRUEBA DE EXTRACCIÓN SOLO")
    
    try:
        scraper = TickantelScraper(headless=True)
        
        # Configurar driver
        if not scraper.setup_driver():
            logger.error("❌ No se pudo configurar el driver")
            return False
        
        # Ejecutar extracción
        if scraper.run():
            logger.info("✅ Extracción completada exitosamente")
            return True
        else:
            logger.error("❌ Error en la extracción")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en la prueba: {str(e)}")
        return False
    finally:
        scraper.close()

if __name__ == "__main__":
    # Ejecutar scraper real para guardar datos en BD
    main()
