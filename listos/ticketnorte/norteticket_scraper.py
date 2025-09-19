from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
import pytz
from dateutil import parser
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection
import requests

# Configurar logging para Airflow (solo stream handler)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class NorteTicketScraper:
    def __init__(self, headless=True, test_mode=False):
        """
        Inicializa el scraper de NorteTicket para Airflow - VERSION MEJORADA CON VENTAS DIARIAS
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless (por defecto True para Airflow)
            test_mode (bool): Si True, solo extrae datos sin guardar en BD
        """
        self.driver = None
        self.headless = headless
        self.test_mode = test_mode
        self.base_url = "https://norteticket.com/auth/login/"
        self.username = "airbag"
        self.password = "norteticket"
        
        # Configuración para Airflow (sin archivos físicos)
        self.download_folder = "/tmp"
        
        # Configuración de base de datos
        self.db_connection = None
        if not self.test_mode:
            self.setup_database_connection()
        
        # Configuración de evasión de bots
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        
        self.evasion_config = {
            "random_delays": True,
            "human_typing": True,
            "mouse_movement": True,
            "scroll_behavior": True,
            "window_resize": True
        }
        
        # Configuración de base de datos
        self.db_connection = None
        # Conectar BD también en test para permitir matching de shows (no haremos inserts en test)
        self.setup_database_connection()
        
        # Datos finales
        self.final_data = {
            "extraction_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "username_used": "daleplay",
            "url": self.base_url,
            "events_data": []
        }
        
    def setup_database_connection(self):
        """Establece conexión con la base de datos"""
        try:
            self.db_connection = get_database_connection()
            if self.db_connection:
                logger.info("✅ Conexión a base de datos establecida exitosamente")
                return True
            else:
                logger.error("❌ No se pudo establecer conexión a la base de datos")
                return False
        except Exception as e:
            logger.error(f"❌ Error estableciendo conexión a la base de datos: {str(e)}")
            return False
    
    def setup_database_connection(self):
        """Configura la conexión a la base de datos"""
        try:
            self.db_connection = get_database_connection()
            if self.db_connection:
                logger.info("✅ Conexión a base de datos establecida exitosamente")
                return True
            else:
                logger.error("❌ No se pudo establecer conexión a la base de datos")
                return False
        except Exception as e:
            logger.error(f"❌ Error configurando conexión BD: {e}")
            return False
    
    def setup_driver(self):
        """Configura el driver de Chrome con evasión de bots para Airflow"""
        try:
            chrome_options = Options()
            
            # Modo headless para Airflow
            if self.headless:
                chrome_options.add_argument("--headless")
            
            # Opciones básicas para contenedores
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            
            # Evasión de bots
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # User agent aleatorio
            user_agent = random.choice(self.user_agents)
            chrome_options.add_argument(f"--user-agent={user_agent}")
            
            # Configurar carpeta de descargas para Airflow
            chrome_options.add_experimental_option("prefs", {
                "download.default_directory": self.download_folder,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })
            
            # Configurar el driver
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con configuración simple: {str(e)}")
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Ocultar que es un bot
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("✅ Driver de Chrome configurado exitosamente con evasión de bots")
            return True
            
        except Exception as e:
            logger.error(f"Error al configurar el driver: {str(e)}")
            return False
    
    def random_delay(self, min_seconds=1, max_seconds=3):
        """Aplica un delay aleatorio para simular comportamiento humano"""
        if self.evasion_config.get("random_delays", False):
            delay = random.uniform(min_seconds, max_seconds)
            time.sleep(delay)
    
    def human_typing(self, element, text):
        """Simula escritura humana con delays aleatorios"""
        if self.evasion_config.get("human_typing", False):
            element.clear()
            for char in text:
                element.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
        else:
            element.clear()
            element.send_keys(text)
    
    def random_mouse_movement(self):
        """Simula movimiento aleatorio del mouse"""
        if self.evasion_config.get("mouse_movement", False):
            try:
                # Mover el mouse a una posición aleatoria
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                self.driver.execute_script(f"window.scrollTo({x}, {y});")
            except:
                pass
    
    def random_scroll(self):
        """Simula scroll aleatorio"""
        if self.evasion_config.get("scroll_behavior", False):
            try:
                scroll_amount = random.randint(100, 500)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                time.sleep(random.uniform(0.5, 1.5))
            except:
                pass
    
    def navigate_to_login(self):
        """Navega a la página de login de NorteTicket"""
        try:
            logger.info(f"Navegando a: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            logger.info("Página de login cargada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error al navegar a la página de login: {str(e)}")
            return False
    
    def get_page_info(self):
        """Obtiene información básica de la página"""
        try:
            title = self.driver.title
            current_url = self.driver.current_url
            
            logger.info(f"Título de la página: {title}")
            logger.info(f"URL actual: {current_url}")
            
            return {
                'title': title,
                'url': current_url
            }
            
        except Exception as e:
            logger.error(f"Error al obtener información de la página: {str(e)}")
            return None
    
    def debug_page_elements(self):
        """Debug: Muestra todos los elementos de input en la página"""
        try:
            logger.info("=== DEBUG: Analizando elementos de la página ===")
            
            # Buscar todos los inputs
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            logger.info(f"Total de inputs encontrados: {len(inputs)}")
            
            for i, input_elem in enumerate(inputs):
                try:
                    input_type = input_elem.get_attribute("type")
                    input_name = input_elem.get_attribute("name")
                    input_id = input_elem.get_attribute("id")
                    input_class = input_elem.get_attribute("class")
                    
                    logger.info(f"Input {i+1}: type='{input_type}', name='{input_name}', id='{input_id}', class='{input_class}'")
                except:
                    continue
            
            # Buscar todos los botones
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            logger.info(f"Total de botones encontrados: {len(buttons)}")
            
            for i, button in enumerate(buttons):
                try:
                    button_text = button.text
                    button_type = button.get_attribute("type")
                    button_class = button.get_attribute("class")
                    
                    logger.info(f"Botón {i+1}: text='{button_text}', type='{button_type}', class='{button_class}'")
                except:
                    continue
            
            logger.info("=== FIN DEBUG ===")
            
        except Exception as e:
            logger.error(f"Error en debug: {str(e)}")
    
    def wait_for_login_form(self):
        """Espera a que cargue el formulario de login"""
        try:
            logger.info("Esperando a que cargue el formulario de login...")
            
            # Esperar más tiempo para que la página cargue completamente
            time.sleep(5)
            
            # Usar los selectores exactos que encontramos en el debug
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_username"))
            )
            logger.info("Campo de usuario encontrado con selector: (By.ID, 'id_username')")
            
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_password"))
            )
            logger.info("Campo de contraseña encontrado con selector: (By.ID, 'id_password')")
            
            logger.info("Formulario de login cargado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error esperando el formulario de login: {str(e)}")
            return False
    
    def login(self, username, password):
        """Realiza el login con las credenciales proporcionadas"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Esperar a que el formulario esté listo
            if not self.wait_for_login_form():
                return False
            
            # Buscar y llenar el campo de username usando el selector correcto
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_username"))
            )
            self.human_typing(username_field, username)
            logger.info(f"Usuario ingresado: {username}")
            self.random_delay(0.5, 1.5)
            
            # Buscar y llenar el campo de contraseña
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_password"))
            )
            self.human_typing(password_field, password)
            logger.info("Contraseña ingresada")
            self.random_delay(0.5, 1.5)
            
            # Buscar y hacer clic en el botón "ACCEDER" usando el selector correcto
            login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' and contains(text(), 'ACCEDER')]"))
            )
            login_button.click()
            logger.info("Botón ACCEDER clickeado")
            
            # Esperar un momento para que procese el login
            time.sleep(5)
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL después del login: {current_url}")
            
            # Verificar si hay mensajes de error
            try:
                error_messages = self.driver.find_elements(By.CLASS_NAME, "alert-danger")
                if error_messages:
                    for error in error_messages:
                        logger.warning(f"Mensaje de error: {error.text}")
                    return False
            except:
                pass
            
            if "login" not in current_url.lower():
                logger.info("Login exitoso - Redirigido a otra página")
                return True
            else:
                logger.warning("Login posiblemente fallido - Aún en página de login")
                return False
                
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def close(self):
        """Cierra el driver del navegador y la conexión a la base de datos"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")
        
        if self.db_connection:
            self.db_connection.close()
            logger.info("🔌 Conexión a base de datos cerrada")
    
    def get_active_events_for_daily_sales(self):
        """
        Obtiene eventos activos con sus IDs para extracción de ventas diarias
        """
        try:
            logger.info("🔍 Obteniendo eventos activos para ventas diarias...")
            
            # Navegar a página de eventos en curso
            self.driver.get("https://norteticket.com/administracion/")
            time.sleep(2)
            
            eventos = []
            
            # Buscar tabla de eventos
            tabla_eventos = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
            )
            
            filas = tabla_eventos.find_elements(By.TAG_NAME, "tr")[1:]  # Saltar header
            
            for fila in filas:
                try:
                    celdas = fila.find_elements(By.TAG_NAME, "td")
                    if len(celdas) >= 4:
                        # Extraer nombre del evento
                        nombre_evento = celdas[0].text.strip()
                        
                        # Buscar link del "ojito" para obtener ID
                        links = celdas[-1].find_elements(By.TAG_NAME, "a")
                        event_id = None
                        
                        for link in links:
                            href = link.get_attribute("href")
                            if href and "/administracion/evento/" in href:
                                # Extraer ID del URL
                                match = re.search(r'/administracion/evento/(\d+)', href)
                                if match:
                                    event_id = match.group(1)
                                    break
                        
                        if event_id and nombre_evento:
                            eventos.append({
                                'id': event_id,
                                'name': nombre_evento
                            })
                            logger.info(f"   ✅ Evento: {nombre_evento} (ID: {event_id})")
                
                except Exception as e:
                    logger.warning(f"❌ Error procesando fila: {e}")
                    continue
            
            logger.info(f"✅ Total eventos encontrados: {len(eventos)}")
            return eventos
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo eventos activos: {e}")
            return []
    
    def extract_daily_sales_from_api(self, event_id):
        """
        Extrae datos del endpoint /api/cache/estadisticas/{event_id}/ - IDÉNTICO AL SCRAPER AIRBAG
        """
        try:
            import requests
            
            # Crear sesión con cookies de Selenium
            session = requests.Session()
            selenium_cookies = self.driver.get_cookies()
            for cookie in selenium_cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            # Headers para simular browser
            session.headers.update({
                'User-Agent': self.driver.execute_script("return navigator.userAgent;"),
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': f'https://norteticket.com/administracion/evento/{event_id}'
            })
            
            # Endpoint de estadísticas
            endpoint = f"https://norteticket.com/api/cache/estadisticas/{event_id}/"
            
            response = session.get(endpoint, timeout=15)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info(f"✅ Datos obtenidos del endpoint para evento {event_id}")
                    return data
                except Exception as e:
                    logger.error(f"❌ Error parseando JSON: {e}")
                    return None
            else:
                logger.error(f"❌ Error HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error accediendo al endpoint: {e}")
            return None
    
    def find_show_id_by_event_name(self, event_name, event_id):
        """
        Busca automáticamente el show_id en la BD basándose en el nombre del evento
        """
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos para buscar show_id")
                return None
            
            cursor = self.db_connection.cursor()
            
            # Extraer el artista del nombre del evento
            # Ejemplo: "LALI EN TRELEW" -> artista="LALI", venue contiene "TRELEW"
            event_parts = event_name.split(" EN ")
            if len(event_parts) >= 2:
                artista_candidato = event_parts[0].strip()
                venue_candidato = event_parts[1].strip()
            else:
                # Si no tiene " EN ", usar el nombre completo como artista
                artista_candidato = event_name.strip()
                venue_candidato = ""
            
            logger.info(f"🔍 Buscando show para: artista='{artista_candidato}', venue contiene '{venue_candidato}'")
            
            # Buscar en la BD con diferentes estrategias
            search_queries = []
            
            # 1. Búsqueda exacta por artista y venue
            if venue_candidato:
                search_queries.append({
                    "query": """
                        SELECT id, artista, venue, fecha_show, capacidad_total 
                        FROM shows 
                        WHERE UPPER(artista) = UPPER(%s) 
                        AND UPPER(venue) LIKE UPPER(%s)
                        AND ticketera = 'ticketnorte'
                        ORDER BY fecha_show DESC
                        LIMIT 1
                    """,
                    "params": (artista_candidato, f"%{venue_candidato}%"),
                    "description": f"Búsqueda exacta: artista='{artista_candidato}' + venue contiene '{venue_candidato}'"
                })
            
            # 2. Búsqueda solo por artista
            search_queries.append({
                "query": """
                    SELECT id, artista, venue, fecha_show, capacidad_total 
                    FROM shows 
                    WHERE UPPER(artista) = UPPER(%s)
                    AND ticketera = 'ticketnorte'
                    ORDER BY fecha_show DESC
                    LIMIT 3
                """,
                "params": (artista_candidato,),
                "description": f"Búsqueda por artista: '{artista_candidato}'"
            })
            
            # 3. Búsqueda fuzzy por nombre completo del evento
            search_queries.append({
                "query": """
                    SELECT id, artista, venue, fecha_show, capacidad_total 
                    FROM shows 
                    WHERE (UPPER(artista) LIKE UPPER(%s) OR UPPER(venue) LIKE UPPER(%s))
                    AND ticketera = 'ticketnorte'
                    ORDER BY fecha_show DESC
                    LIMIT 3
                """,
                "params": (f"%{artista_candidato}%", f"%{venue_candidato if venue_candidato else artista_candidato}%"),
                "description": f"Búsqueda fuzzy en artista/venue"
            })
            
            # Ejecutar búsquedas en orden de precisión
            for i, search in enumerate(search_queries, 1):
                logger.info(f"🔍 Estrategia {i}: {search['description']}")
                cursor.execute(search["query"], search["params"])
                results = cursor.fetchall()
                
                if results:
                    # Si encontramos resultados, tomar el primero (más reciente)
                    show = results[0]
                    show_id = show[0]
                    db_artista = show[1]
                    db_venue = show[2]
                    db_fecha = show[3]
                    db_capacidad = show[4]
                    
                    logger.info(f"✅ MATCH ENCONTRADO:")
                    logger.info(f"   Show ID: {show_id}")
                    logger.info(f"   Artista BD: {db_artista}")
                    logger.info(f"   Venue BD: {db_venue}")
                    logger.info(f"   Fecha: {db_fecha}")
                    logger.info(f"   Capacidad: {db_capacidad}")
                    
                    print(f"   ✅ Match: {db_artista} en {db_venue} (ID: {show_id})")
                    return show_id
                else:
                    logger.info(f"   ❌ Sin resultados con estrategia {i}")
            
            # Si no encontramos nada
            logger.warning(f"⚠️ No se encontró show_id para evento: '{event_name}' (ID: {event_id})")
            print(f"   ⚠️ No se encontró match en BD para: {event_name}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error buscando show_id para '{event_name}': {e}")
            return None
    
    def upsert_daily_sale_real(self, show_id, fecha_venta, venta_diaria, monto_diario, venta_total_acumulada, recaudacion_total, ticketera, url_origen=None, precio_promedio_ars=0):
        """
        Actualiza o inserta registro de ventas diarias con TODOS los campos - IGUAL QUE TUENTRADA
        Retorna: "updated", "inserted", "skipped", "error"
        """
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos")
                return "error"
            
            cursor = self.db_connection.cursor()
            
            # Obtener la capacidad total del show desde la tabla shows (IGUAL QUE TUENTRADA)
            capacity_query = """
                SELECT capacidad_total FROM shows WHERE id = %s
            """
            cursor.execute(capacity_query, (show_id,))
            capacity_result = cursor.fetchone()
            
            if capacity_result:
                capacidad_total = capacity_result[0]
                logger.debug(f"📊 Capacidad total del show: {capacidad_total}")
            else:
                logger.warning(f"⚠️ No se encontró capacidad para show_id: {show_id}")
                capacidad_total = 0
            
            # Calcular tickets disponibles y ocupación (IGUAL QUE TUENTRADA)
            tickets_disponibles = capacidad_total - venta_total_acumulada if capacidad_total > 0 else 0
            porcentaje_ocupacion = (venta_total_acumulada / capacidad_total * 100) if capacidad_total > 0 else 0.0
            
            # Verificar si ya existe el registro
            check_query = """
                SELECT id, venta_diaria FROM daily_sales 
                WHERE show_id = %s AND fecha_venta = %s
            """
            cursor.execute(check_query, (show_id, fecha_venta))
            existing_record = cursor.fetchone()
            
            if existing_record:
                existing_id, existing_venta = existing_record
                
                # Actualizar SIEMPRE todos los campos para mantener consistencia
                update_query = """
                    UPDATE daily_sales SET
                        venta_diaria = %s,
                        monto_diario_ars = %s,
                        precio_promedio_ars = %s,
                        venta_total_acumulada = %s,
                        recaudacion_total_ars = %s,
                        tickets_disponibles = %s,
                        porcentaje_ocupacion = %s,
                        fecha_extraccion = %s,
                        ticketera = %s,
                        url_origen = %s,
                        updated_at = NOW()
                    WHERE show_id = %s AND fecha_venta = %s
                """
                cursor.execute(update_query, (
                    venta_diaria,
                    int(monto_diario),
                    int(precio_promedio_ars or 0),
                    venta_total_acumulada,
                    int(recaudacion_total),
                    tickets_disponibles,
                    round(porcentaje_ocupacion, 2),
                    datetime.now(),
                    ticketera,
                    url_origen,
                    show_id,
                    fecha_venta
                ))
                self.db_connection.commit()
                return "updated"
            else:
                # Insertar nuevo registro con TODOS los campos (IGUAL QUE TUENTRADA)
                insert_query = """
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion,
                        venta_diaria, monto_diario_ars, precio_promedio_ars,
                        venta_total_acumulada, recaudacion_total_ars,
                        tickets_disponibles, porcentaje_ocupacion,
                        ticketera, archivo_origen, url_origen
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                cursor.execute(insert_query, (
                    show_id,
                    fecha_venta,
                    datetime.now(),
                    venta_diaria,
                    int(monto_diario),
                    int(precio_promedio_ars or 0),
                    venta_total_acumulada,
                    int(recaudacion_total),
                    tickets_disponibles,
                    round(porcentaje_ocupacion, 2),
                    ticketera,
                    'norteticket_scraper.py',
                    url_origen
                ))
                self.db_connection.commit()
                return "inserted"
                
        except Exception as e:
            logger.error(f"❌ Error en upsert_daily_sale_real: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return "error"
    
    def update_daily_sales_from_endpoint(self):
        """
        Actualiza TODAS las ventas diarias usando el endpoint - LÓGICA IDÉNTICA AL SCRAPER AIRBAG
        Se ejecuta cada vez que corre el scraper para mantener datos sincronizados
        """
        try:
            logger.info("📊 EXTRAYENDO VENTAS DIARIAS DEL ENDPOINT PARA ACTUALIZAR BD...")
            print("\n📊 ACTUALIZANDO VENTAS DIARIAS DESDE ENDPOINT")
            print("=" * 60)
            
            # 1. Obtener eventos activos
            eventos_activos = self.get_active_events_for_daily_sales()
            
            if not eventos_activos:
                print("❌ No se encontraron eventos activos")
                return False
            
            total_updates = 0
            
            # 2. Para cada evento, procesar ventas diarias con la MISMA lógica del AIRBAG
            for i, evento in enumerate(eventos_activos, 1):
                event_id = evento.get('id')
                event_name = evento.get('name', 'Sin nombre')
                
                print(f"\n--- EVENTO {i}/{len(eventos_activos)}: {event_name} (ID: {event_id}) ---")
                
                # Extraer datos del endpoint (igual que AIRBAG)
                daily_data = self.extract_daily_sales_from_api(event_id)
                
                if not daily_data:
                    logger.error(f"❌ No se pudieron extraer datos del endpoint para evento {event_id}")
                    print(f"❌ Sin datos para: {event_name}")
                    continue
                
                # BÚSQUEDA AUTOMÁTICA DE SHOW_ID EN LA BD
                print(f"🔍 Buscando match en BD para: {event_name}")
                show_id = self.find_show_id_by_event_name(event_name, event_id)
                
                if not show_id:
                    logger.warning(f"⚠️ No se encontró show_id para evento {event_id}, saltando...")
                    print(f"⚠️ Sin match en BD, saltando evento...")
                    continue
                
                # Procesar datos (IGUAL QUE AIRBAG)
                labels = daily_data.get('labels', [])
                ventas_online = daily_data.get('ventas_online', [])
                ventas_pdv = daily_data.get('ventas_pdv', [])
                
                if not labels or not ventas_online:
                    logger.error(f"❌ Datos del endpoint incompletos para evento {event_id}")
                    print(f"❌ Datos incompletos para: {event_name}")
                    continue
                
                logger.info(f"📅 Procesando {len(labels)} días de datos para {event_name}...")
                print(f"📅 Procesando {len(labels)} días de datos...")
                
                # Procesar cada día (LÓGICA EXACTA DEL AIRBAG)
                updates_count = 0
                from datetime import datetime, date
                
                for j, fecha_str in enumerate(labels):
                    try:
                        # Convertir fecha DD/MM/YY a YYYY-MM-DD (IGUAL QUE AIRBAG)
                        fecha_parts = fecha_str.split('/')
                        if len(fecha_parts) == 3:
                            day, month, year = fecha_parts
                            if len(year) == 2:
                                year = f"20{year}"
                            fecha_venta = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        else:
                            continue
                        
                        # Obtener ventas del día (IGUAL QUE AIRBAG)
                        venta_online = ventas_online[j] if j < len(ventas_online) else 0
                        venta_pdv = ventas_pdv[j] if j < len(ventas_pdv) else 0
                        venta_total = venta_online + venta_pdv
                        
                        # Solo procesar días con ventas O días recientes (IGUAL QUE AIRBAG)
                        fecha_obj = datetime.strptime(fecha_venta, '%Y-%m-%d').date()
                        hoy = date.today()
                        dias_diferencia = (hoy - fecha_obj).days
                        
                        if venta_total > 0 or dias_diferencia <= 30:
                            # Calcular ventas totales acumuladas hasta la fecha (IGUAL QUE TUENTRADA)
                            venta_total_acumulada = sum(
                                (ventas_online[k] if k < len(ventas_online) else 0) + 
                                (ventas_pdv[k] if k < len(ventas_pdv) else 0) 
                                for k in range(j + 1)  # Sumar hasta el día actual (inclusive)
                            )
                            
                            # Calcular montos (asumiendo precio promedio de $50,000 por ticket)
                            precio_promedio = 50000  # Ajustar según el evento real
                            monto_diario = venta_total * precio_promedio
                            recaudacion_total = venta_total_acumulada * precio_promedio
                            
                            # UPSERT REAL usando la conexión de base de datos con TODOS los campos
                            result = self.upsert_daily_sale_real(
                                show_id=show_id,
                                fecha_venta=fecha_venta,
                                venta_diaria=venta_total,
                                monto_diario=monto_diario,
                                venta_total_acumulada=venta_total_acumulada,
                                recaudacion_total=recaudacion_total,
                                ticketera='norteticket'
                            )
                            
                            if result == "updated":
                                logger.info(f"🔄 {fecha_venta}: ACTUALIZADO a {venta_total} ventas")
                                print(f"   🔄 {fecha_venta}: ACTUALIZADO a {venta_total} ventas")
                                updates_count += 1
                            elif result == "inserted":
                                logger.info(f"➕ {fecha_venta}: INSERTADO {venta_total} ventas")
                                print(f"   ➕ {fecha_venta}: INSERTADO {venta_total} ventas")
                                updates_count += 1
                            elif result == "skipped":
                                logger.info(f"⏭️ {fecha_venta}: SALTEADO (sin cambios)")
                                print(f"   ⏭️ {fecha_venta}: OK (sin cambios)")
                            else:
                                logger.warning(f"⚠️ {fecha_venta}: Error en actualización")
                    
                    except Exception as e:
                        logger.warning(f"⚠️ Error procesando fecha {fecha_str}: {e}")
                        continue
                
                print(f"✅ {event_name}: {updates_count} días actualizados")
                total_updates += updates_count
            
            print(f"\n📊 RESUMEN DE ACTUALIZACIÓN:")
            print(f"   📅 Total eventos procesados: {len(eventos_activos)}")
            print(f"   ✅ Total días actualizados: {total_updates}")
            print(f"   💾 Base de datos sincronizada")
            
            logger.info(f"✅ Ventas diarias actualizadas: {total_updates} registros procesados")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error actualizando ventas diarias: {e}")
            return False
    
    def process_daily_sales_for_upsert(self, daily_data, event_name):
        """
        Procesa los datos de ventas diarias para hacer UPSERT en la BD
        """
        try:
            # Por ahora, simulamos el procesamiento
            # En una implementación completa, aquí buscaríamos el show_id 
            # correspondiente en la BD y haríamos UPSERT de cada día
            
            ventas_diarias = daily_data.get('ventas_diarias', [])
            updates_count = 0
            
            for venta in ventas_diarias:
                if venta['total_dia'] > 0:  # Solo procesar días con ventas
                    updates_count += 1
            
            logger.info(f"📊 {event_name}: {updates_count} días con ventas procesados")
            
        except Exception as e:
            logger.error(f"❌ Error procesando ventas para UPSERT: {e}")
    
    def get_eventos_en_curso(self):
        """Obtiene la lista de eventos en curso desde la tabla"""
        try:
            logger.info("Obteniendo eventos en curso...")
            
            # Intentar esperar por id específico, si no aparece, fallback genérico
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.ID, "eventos_en_curso"))
                )
                # Esperar a que termine el procesamiento de DataTables si existe
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.invisibility_of_element_located((By.ID, "eventos_en_curso_processing"))
                    )
                except Exception:
                    pass
                # Esperar a que haya al menos una fila
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#eventos_en_curso tbody tr"))
                )
                rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            except Exception:
                # Esperar por cualquier enlace de 'ojito' a administracion/evento/{id}
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/administracion/evento/']"))
                )
                # Tomar filas que contengan ese anchor
                ojo_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/administracion/evento/']")
                rows = []
                for link in ojo_links:
                    try:
                        row = link.find_element(By.XPATH, "ancestor::tr")
                        rows.append(row)
                    except Exception:
                        continue
            logger.info(f"Encontrados {len(rows)} eventos en curso (determinados por enlaces de 'ojito')")
            
            eventos = []
            for i, row in enumerate(rows):
                try:
                    # Obtener información del evento
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    # Si no hay <a> con texto, tomar texto plano
                    link_in_name = None
                    try:
                        link_in_name = evento_cell.find_element(By.TAG_NAME, "a")
                        evento_nombre = link_in_name.text.strip()
                    except Exception:
                        evento_nombre = evento_cell.text.strip()
                    
                    # Limpiar el nombre del evento (remover información adicional como "Total Entradas Emitidas")
                    if "\n" in evento_nombre:
                        evento_nombre = evento_nombre.split("\n")[0].strip()
                    
                    # Obtener fecha del evento
                    fecha_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                    fecha_evento = fecha_cell.text.strip()
                    
                    # Obtener recinto y ciudad
                    recinto_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)")
                    recinto = recinto_cell.text.strip()
                    
                    ciudad_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(4)")
                    ciudad = ciudad_cell.text.strip()
                    
                    # Buscar enlaces de borderaux (silla) y ojito si existen
                    borderaux_url = ""
                    ojito_url = ""
                    try:
                        borderaux_link = row.find_element(By.CSS_SELECTOR, "a.btn-circle.btn-outline-primary i.fa.fa-chair").find_element(By.XPATH, "./..")
                        borderaux_url = borderaux_link.get_attribute("href")
                    except Exception:
                        pass
                    try:
                        eye_link = row.find_element(By.CSS_SELECTOR, "td:last-child a[href*='/administracion/evento/']")
                        ojito_url = eye_link.get_attribute("href")
                    except Exception:
                        pass
                    
                    evento_info = {
                        'indice': i + 1,
                        'nombre': evento_nombre,
                        'fecha': fecha_evento,
                        'recinto': recinto,
                        'ciudad': ciudad,
                        'borderaux_url': borderaux_url,
                        'ojito_url': ojito_url
                    }
                    
                    eventos.append(evento_info)
                    logger.info(f"Evento {i+1}: {evento_nombre} - {fecha_evento} - {ciudad}")
                    
                except Exception as e:
                    logger.error(f"Error procesando fila {i+1}: {str(e)}")
                    continue
            
            return eventos
            
        except Exception as e:
            logger.error(f"Error obteniendo eventos en curso: {str(e)}")
            return []
    
    def click_borderaux(self, evento_info):
        """Hace clic en el botón borderaux (ícono de silla) de un evento específico"""
        try:
            logger.info(f"Haciendo clic en Borderaux para: {evento_info['nombre']}")
            
            # Buscar la fila del evento por su nombre
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            
            for row in rows:
                try:
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    evento_nombre = evento_cell.find_element(By.TAG_NAME, "a").text.strip()
                    
                    # Limpiar el nombre del evento (igual que en get_eventos_en_curso)
                    if "\n" in evento_nombre:
                        evento_nombre = evento_nombre.split("\n")[0].strip()
                    
                    logger.info(f"Comparando: '{evento_nombre}' con '{evento_info['nombre']}'")
                    
                    if evento_nombre == evento_info['nombre']:
                        # Encontrar y hacer clic en el botón borderaux (ícono de silla)
                        borderaux_link = row.find_element(By.CSS_SELECTOR, "a.btn-circle.btn-outline-primary i.fa.fa-chair").find_element(By.XPATH, "./..")
                        borderaux_link.click()
                        logger.info(f"✅ Clic exitoso en Borderaux para: {evento_info['nombre']}")
                        
                        # Esperar a que la página cargue
                        time.sleep(3)
                        return True
                        
                except Exception as e:
                    logger.error(f"Error procesando fila: {str(e)}")
                    continue
            
            logger.error(f"No se pudo encontrar el evento: {evento_info['nombre']}")
            return False
            
        except Exception as e:
            logger.error(f"Error haciendo clic en borderaux: {str(e)}")
            return False

    def click_ojito(self, evento_info):
        """Hace clic en el botón de ver (ícono de ojito) de un evento específico"""
        try:
            logger.info(f"Haciendo clic en Ojito para: {evento_info['nombre']}")
            
            # Buscar la fila del evento por su nombre
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            if not rows:
                rows = self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            
            for row in rows:
                try:
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    evento_nombre = evento_cell.find_element(By.TAG_NAME, "a").text.strip()
                    
                    # Limpiar el nombre del evento (igual que en get_eventos_en_curso)
                    if "\n" in evento_nombre:
                        evento_nombre = evento_nombre.split("\n")[0].strip()
                    
                    logger.info(f"Comparando: '{evento_nombre}' con '{evento_info['nombre']}'")
                    
                    if evento_nombre == evento_info['nombre']:
                        # Encontrar y hacer clic en el botón ojito (ícono de ojo)
                        eye_link = None
                        href = None
                        # Intento 1: por ícono
                        try:
                            eye_link = row.find_element(By.CSS_SELECTOR, "td:last-child a.btn.btn-circle.btn-outline-primary i.fas.fa-eye").find_element(By.XPATH, "./..")
                        except Exception:
                            pass
                        # Intento 2: por href que apunte a administracion/evento/
                        if eye_link is None:
                            candidates = row.find_elements(By.CSS_SELECTOR, "td:last-child a[href*='/administracion/evento/']")
                            if candidates:
                                eye_link = candidates[0]
                        if eye_link is None:
                            logger.warning("No se encontró el enlace del ojito en la fila")
                            return False
                        href = eye_link.get_attribute("href")
                        eye_link.click()
                        logger.info(f"✅ Clic exitoso en Ojito para: {evento_info['nombre']} | URL: {href}")
                        
                        # Esperar a que la página cargue
                        time.sleep(3)
                        return True
                        
                except Exception as e:
                    logger.error(f"Error procesando fila: {str(e)}")
                    continue
            
            logger.error(f"No se pudo encontrar el evento para ojito: {evento_info['nombre']}")
            return False
            
        except Exception as e:
            logger.error(f"Error haciendo clic en ojito: {str(e)}")
            return False

    def inspect_ojito_page(self):
        """Inspecciona y muestra SOLO Total Ventas y Ventas Diarias en la página del 'ojito'."""
        try:
            # Extraer métricas de cards del bloque "Resumen Diario": Total Ventas (monto) y Ventas Diarias (tickets)
            try:
                # Esperar a que aparezca el bloque Resumen Diario
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//*[contains(normalize-space(.),'Resumen Diario')]"))
                    )
                except Exception:
                    pass

                def get_card_value_in_resumen_diario(header_text: str):
                    try:
                        # Buscar todas las cards cuyo header coincida tras el bloque "Resumen Diario"
                        candidates = self.driver.find_elements(
                            By.XPATH,
                            "//*[contains(normalize-space(.),'Resumen Diario')]/following::div[contains(@class,'card')][.//div[contains(@class,'card-header')][contains(normalize-space(.), '" + header_text + "')]]"
                        )
                        if not candidates:
                            return ""
                        # Elegir la primera visible
                        card = next((c for c in candidates if c.is_displayed()), candidates[0])
                        val_el = card.find_element(By.XPATH, ".//div[contains(@class,'card-title')]")
                        text = val_el.text.strip()
                        retries = 0
                        while (not text or text in ["-", "$"]) and retries < 8:
                            time.sleep(0.4)
                            text = val_el.text.strip()
                            retries += 1
                        return text
                    except Exception:
                        return ""

                # Total Ventas (monto)
                total_ventas_val = None
                try:
                    total_ventas_text = get_card_value_in_resumen_diario('Total Ventas')
                    total_ventas_val = self.parse_currency(total_ventas_text)
                except Exception:
                    pass

                # Ventas Diarias (cantidad)
                ventas_diarias_val = None
                ventas_diarias_text = None
                try:
                    ventas_diarias_text = get_card_value_in_resumen_diario('Ventas Diarias')
                    # Si viene con símbolo de moneda, no es tickets; lo resolveremos por API
                    if re.search(r"[$]|ARS", ventas_diarias_text):
                        ventas_diarias_val = None
                    else:
                        ventas_diarias_val = self.parse_number(ventas_diarias_text)
                except Exception:
                    pass

                # No usar endpoint como fallback en esta etapa: solo DOM

                # Imprimir resumen
                if total_ventas_val is not None:
                    print(f"Total Ventas (monto): {int(total_ventas_val)}")
                if ventas_diarias_val is not None:
                    print(f"Ventas Diarias (tickets): {ventas_diarias_val}")
            except Exception as e:
                logger.debug(f"No se pudieron extraer cards de métricas: {e}")

            print("")
            return True
        except Exception as e:
            logger.warning(f"Inspección fallida: {e}")
            return False

    def extract_daily_metrics_from_dom(self):
        """Devuelve (monto_diario_ars, venta_diaria_tickets) desde Resumen Diario."""
        total_ventas_val = None
        ventas_diarias_val = None
        try:
            # Reusar lógica de inspect_ojito_page pero devolviendo valores
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(normalize-space(.),'Resumen Diario')]"))
                )
            except Exception:
                pass
            def get_card_value_in_resumen_diario(header_text: str):
                try:
                    candidates = self.driver.find_elements(
                        By.XPATH,
                        "//*[contains(normalize-space(.),'Resumen Diario')]/following::div[contains(@class,'card')][.//div[contains(@class,'card-header')][contains(normalize-space(.), '" + header_text + "')]]"
                    )
                    if not candidates:
                        return ""
                    card = next((c for c in candidates if c.is_displayed()), candidates[0])
                    val_el = card.find_element(By.XPATH, ".//div[contains(@class,'card-title')]")
                    text = val_el.text.strip()
                    retries = 0
                    while (not text or text in ["-", "$"]) and retries < 8:
                        time.sleep(0.4)
                        text = val_el.text.strip()
                        retries += 1
                    return text
                except Exception:
                    return ""
            total_text = get_card_value_in_resumen_diario('Total Ventas')
            diarias_text = get_card_value_in_resumen_diario('Ventas Diarias')
            total_ventas_val = int(self.parse_currency(total_text)) if total_text else 0
            if re.search(r"[$]|ARS", diarias_text):
                ventas_diarias_val = None
            else:
                ventas_diarias_val = int(self.parse_number(diarias_text)) if diarias_text else 0
        except Exception:
            pass
        return total_ventas_val or 0, ventas_diarias_val if ventas_diarias_val is not None else 0

    def ingest_event_daily_sales(self, evento_info, test_only=True):
        """Abre ojito, lee métricas, calcula acumulados y hace upsert (o imprime si test_only)."""
        try:
            # Preferir navegar directamente por URL capturada en la fila
            ojito_url = evento_info.get('ojito_url') if isinstance(evento_info, dict) else None
            if ojito_url:
                # Completar URL si viene relativa
                if ojito_url.startswith('/'):
                    ojito_url = f"https://norteticket.com{ojito_url}"
                self.driver.get(ojito_url)
                time.sleep(2)
            else:
                if not self.click_ojito(evento_info):
                    # Fallback global por href si el clic por fila falla
                    if not self.click_first_ojito_global():
                        print("❌ No se pudo abrir el ojito para ingesta")
                        return False
            # Métricas del día
            monto_diario_ars, venta_diaria = self.extract_daily_metrics_from_dom()
            # Match BD
            show_id = self.match_show_by_artist_and_date(evento_info['nombre'], evento_info['fecha'])
            if not show_id:
                print("⚠️ Sin match en BD; no se ingresa")
                self.volver_a_eventos()
                return False
            # Acumulados desde endpoint + capacidad para ocupación/disponibles
            current_url = self.driver.current_url
            event_id = None
            m = re.search(r"/administracion/evento/(\d+)", current_url)
            if m:
                event_id = m.group(1)
            venta_total_acumulada = 0
            recaudacion_total_ars = 0
            capacidad_total = 0
            tickets_disponibles = 0
            porcentaje_ocupacion = 0.0
            precio_promedio = 0
            # Calcular acumulado desde la BD (no desde API)
            try:
                fecha_hoy = self.get_today_date_argentina()
                cur_acc = self.db_connection.cursor()
                # SIEMPRE tomar acumulado del último día previo y sumarle la venta del día actual
                cur_acc.execute(
                    """
                    select venta_total_acumulada
                    from public.daily_sales
                    where show_id=%s and fecha_venta < %s
                    order by fecha_venta desc, updated_at desc
                    limit 1
                    """,
                    (show_id, fecha_hoy)
                )
                row_prev = cur_acc.fetchone()
                prev_acc = int(row_prev[0]) if row_prev and row_prev[0] is not None else 0
                venta_total_acumulada = prev_acc + int(venta_diaria or 0)
                cur_acc.close()
            except Exception:
                # Fallback: si falla, usar solo venta_diaria
                venta_total_acumulada = int(venta_diaria or 0)
            
            # Calcular recaudación total acumulada (suma de montos diarios)
            try:
                fecha_hoy = self.get_today_date_argentina()
                cur_rec = self.db_connection.cursor()
                # Tomar recaudación del último día previo y sumarle el monto del día actual
                cur_rec.execute(
                    """
                    select recaudacion_total_ars
                    from public.daily_sales
                    where show_id=%s and fecha_venta < %s
                    order by fecha_venta desc, updated_at desc
                    limit 1
                    """,
                    (show_id, fecha_hoy)
                )
                row_prev_rec = cur_rec.fetchone()
                prev_recaudacion = int(row_prev_rec[0]) if row_prev_rec and row_prev_rec[0] is not None else 0
                recaudacion_total_ars = prev_recaudacion + int(monto_diario_ars or 0)
                cur_rec.close()
            except Exception:
                # Fallback: si falla, usar solo monto_diario_ars
                recaudacion_total_ars = int(monto_diario_ars or 0)
            
            # Estimar precio promedio del día si es posible
            if venta_diaria > 0 and monto_diario_ars > 0:
                precio_promedio = round(monto_diario_ars / max(venta_diaria, 1))
            # Fallback: si precio_promedio sigue 0, tomar último precio_promedio_ars no nulo del show
            if precio_promedio == 0:
                try:
                    cur2 = self.db_connection.cursor()
                    cur2.execute("""
                        select precio_promedio_ars
                        from public.daily_sales
                        where show_id=%s and precio_promedio_ars is not null and precio_promedio_ars > 0
                        order by fecha_venta desc, updated_at desc
                        limit 1
                    """, (show_id,))
                    r2 = cur2.fetchone()
                    cur2.close()
                    if r2 and r2[0]:
                        precio_promedio = int(r2[0])
                except Exception:
                    pass
                if precio_promedio > 0 and recaudacion_total_ars == 0:
                    recaudacion_total_ars = venta_total_acumulada * precio_promedio
            # Capacidad desde shows
            try:
                cur = self.db_connection.cursor()
                cur.execute("select capacidad_total from public.shows where id=%s", (show_id,))
                r = cur.fetchone()
                if r and r[0]:
                    capacidad_total = int(r[0])
                cur.close()
            except Exception:
                capacidad_total = 0
            if capacidad_total > 0:
                tickets_disponibles = max(capacidad_total - venta_total_acumulada, 0)
                porcentaje_ocupacion = (venta_total_acumulada / capacidad_total) * 100.0
            # Upsert o print
            if test_only:
                fecha_arg = self.get_today_date_argentina()
                print(f"🧪 READY UPSERT → show_id={show_id} fecha={fecha_arg} tickets={venta_diaria} monto={monto_diario_ars} acumulado={venta_total_acumulada} recaudado={recaudacion_total_ars} capacidad={capacidad_total} disp={tickets_disponibles} ocup={round(porcentaje_ocupacion,2)}% precio_prom={precio_promedio}")
            else:
                result = self.upsert_daily_sale_real(
                    show_id=show_id,
                    fecha_venta=self.get_today_date_argentina(),
                    venta_diaria=venta_diaria,
                    monto_diario=monto_diario_ars,
                    venta_total_acumulada=venta_total_acumulada,
                    recaudacion_total=recaudacion_total_ars,
                    ticketera='norteticket',
                    url_origen=current_url,
                    precio_promedio_ars=precio_promedio
                )
                print(f"💾 UPSERT daily_sales → {result}")
            self.volver_a_eventos()
            return True
        except Exception as e:
            print(f"❌ Ingesta fallida: {e}")
            try:
                self.volver_a_eventos()
            except:
                pass
            return False

    def click_first_ojito_global(self):
        """Encuentra y clickea el primer enlace 'ojito' a /administracion/evento/{id} en la página actual."""
        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/administracion/evento/']"))
            )
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/administracion/evento/']")
            if not links:
                logger.warning("No se encontraron enlaces de ojito globales")
                return False
            link = links[0]
            href = link.get_attribute("href")
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", link)
            time.sleep(0.5)
            link.click()
            logger.info(f"✅ Clic global en ojito → {href}")
            time.sleep(2)
            return True
        except Exception as e:
            logger.warning(f"Fallo clic global en ojito: {e}")
            return False

    def parse_event_date_str(self, fecha_str):
        """Convierte 'DD/MM/YY HH:MM' u 'DD/MM/YY' a date (YYYY-MM-DD)."""
        try:
            fecha_part = fecha_str.strip()
            if ' ' in fecha_part:
                fecha_part = fecha_part.split(' ')[0]
            day, month, year = fecha_part.split('/')
            if len(year) == 2:
                year = f"20{year}"
            return datetime(int(year), int(month), int(day)).date()
        except Exception:
            return None

    def get_today_date_argentina(self):
        """Devuelve la fecha de hoy en Argentina (UTC-3), robusto cerca de medianoche."""
        try:
            tz = pytz.timezone('America/Argentina/Buenos_Aires')
            return datetime.now(tz).date()
        except Exception:
            # Fallback: aproximar restando 3 horas a UTC
            return (datetime.utcnow() - timedelta(hours=3)).date()

    def match_show_by_artist_and_date(self, nombre_evento, fecha_evento):
        """Busca show_id en BD por artista (extraído del nombre) y fecha exacta (DATE(fecha_show))."""
        try:
            if not self.db_connection:
                self.setup_database_connection()
            if not self.db_connection:
                return None
            # Extraer artista: tomar parte antes de ' EN '
            artista = nombre_evento.split(' EN ')[0].strip() if ' EN ' in nombre_evento else nombre_evento.strip()
            fecha_date = self.parse_event_date_str(fecha_evento)
            if not fecha_date:
                return None
            cursor = self.db_connection.cursor()
            query = (
                "select id, artista, venue, fecha_show from public.shows "
                "where ticketera='ticketnorte' and upper(artista)=upper(%s) and date(fecha_show)=%s "
                "order by fecha_show desc limit 1"
            )
            cursor.execute(query, (artista, fecha_date))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return row[0]
            return None
        except Exception as e:
            logger.warning(f"Match por artista/fecha falló: {e}")
            return None
    
    def extraer_info_borderaux(self, evento_info):
        """Extrae información de la tabla de borderaux/ventas de la página actual"""
        try:
            logger.info(f"Extrayendo información de borderaux para: {evento_info['nombre']}")
            
            # Esperar a que cargue la página del borderaux
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".table-responsive"))
            )
            
            # Extraer información del encabezado de la tabla
            try:
                nombre_evento_elem = self.driver.find_element(By.ID, "evento")
                nombre_evento = nombre_evento_elem.text.strip()
            except:
                nombre_evento = evento_info['nombre']
            
            try:
                fecha_emision_elem = self.driver.find_element(By.CSS_SELECTOR, ".headers")
                fecha_emision = fecha_emision_elem.text.strip()
            except:
                fecha_emision = ""
            
            # Estructura de datos para el borderaux
            info_borderaux = {
                'nombre_evento': nombre_evento,
                'fecha_evento': evento_info['fecha'],
                'recinto': evento_info['recinto'],
                'ciudad': evento_info['ciudad'],
                'fecha_emision': fecha_emision,
                'categorias': [],
                'resumen_total': {},
                'fecha_extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Extraer datos de la tabla
            try:
                # Buscar todas las filas de datos (excluyendo encabezados y total)
                rows = self.driver.find_elements(By.CSS_SELECTOR, ".table tbody tr:not(#titulos):not(#total)")
                logger.info(f"Encontradas {len(rows)} filas de categorías")
                
                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 8:  # Asegurar que tiene todas las columnas
                            
                            categoria = cells[0].text.strip()
                            cupos = cells[1].text.strip()
                            cupos_libres = cells[2].text.strip()
                            estado = cells[3].text.strip()
                            cantidad_vendida = cells[4].text.strip()
                            precio_punto_venta = cells[5].text.strip()
                            precio_online = cells[6].text.strip()
                            precio_descuento = cells[7].text.strip() if len(cells) > 7 else ""
                            total = cells[8].text.strip() if len(cells) > 8 else cells[7].text.strip()
                            
                            categoria_info = {
                                'categoria': categoria,
                                'cupos': self.limpiar_numero(cupos),
                                'cupos_libres': self.limpiar_numero(cupos_libres),
                                'estado': estado,
                                'cantidad_vendida': self.limpiar_numero(cantidad_vendida),
                                'precio_punto_venta': self.limpiar_precio(precio_punto_venta),
                                'precio_online': self.limpiar_precio(precio_online),
                                'precio_descuento': self.limpiar_precio(precio_descuento),
                                'total': self.limpiar_precio(total)
                            }
                            
                            info_borderaux['categorias'].append(categoria_info)
                            logger.info(f"✅ Categoría extraída: {categoria} - Vendida: {cantidad_vendida} - Total: {total}")
                            
                    except Exception as e:
                        logger.error(f"Error procesando fila de categoría: {str(e)}")
                        continue
                
                # Extraer fila de totales
                try:
                    total_row = self.driver.find_element(By.ID, "total")
                    total_cells = total_row.find_elements(By.TAG_NAME, "td")
                    
                    if len(total_cells) >= 8:
                        info_borderaux['resumen_total'] = {
                            'total_cupos': self.limpiar_numero(total_cells[1].text.strip()),
                            'total_cupos_libres': self.limpiar_numero(total_cells[2].text.strip()),
                            'total_cantidad_vendida': self.limpiar_numero(total_cells[4].text.strip()),
                            'total_general': self.limpiar_precio(total_cells[-1].text.strip())
                        }
                        logger.info(f"✅ Total general extraído: {info_borderaux['resumen_total']['total_general']}")
                        
                except Exception as e:
                    logger.error(f"Error extrayendo totales: {str(e)}")
                    
            except Exception as e:
                logger.error(f"Error extrayendo datos de la tabla: {str(e)}")
            
            # Agregar información al final_data en lugar de guardar archivo
            self.final_data["events_data"].append(info_borderaux)
            
            logger.info(f"✅ Información de borderaux agregada a datos finales")
            logger.info(f"   📊 Evento: {info_borderaux['nombre_evento']}")
            logger.info(f"   📊 Total vendido: {info_borderaux['resumen_total'].get('total_cantidad_vendida', 0)}")
            logger.info(f"   📊 Total recaudado: ${info_borderaux['resumen_total'].get('total_general', 0):,}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error extrayendo información de borderaux: {str(e)}")
            return False
    
    def extraer_valor_monetario(self, texto):
        """Extrae valores monetarios del texto"""
        try:
            # Buscar patrones como "$ 74060000" o "$ 0"
            match = re.search(r'\$\s*([\d,]+)', texto)
            if match:
                return match.group(1).replace(',', '')
            return "0"
        except:
            return "0"
    
    def limpiar_numero(self, texto):
        """Limpia y extrae números del texto"""
        try:
            # Remover espacios y buscar números
            texto_limpio = texto.strip()
            match = re.search(r'(\d+)', texto_limpio)
            if match:
                return int(match.group(1))
            return 0
        except:
            return 0
    
    def limpiar_precio(self, texto):
        """Limpia y extrae valores de precio del texto"""
        try:
            # Remover espacios y buscar precios
            texto_limpio = texto.strip()
            if texto_limpio == "-" or texto_limpio == "":
                return 0
            
            # Buscar patrones como "ARS 55000" o "$ 270325000"
            match = re.search(r'(?:ARS|[$])\s*([\d,]+)', texto_limpio)
            if match:
                return int(match.group(1).replace(',', ''))
            
            # Si solo contiene números
            match = re.search(r'(\d+)', texto_limpio)
            if match:
                return int(match.group(1))
            
            return 0
        except:
            return 0
    
    def extraer_numero(self, texto):
        """Extrae números del texto"""
        try:
            # Buscar números en el texto
            match = re.search(r'(\d+)', texto)
            if match:
                return match.group(1)
            return "0"
        except:
            return "0"
    
    def parse_number(self, text):
        """Convierte texto a número entero"""
        try:
            if isinstance(text, (int, float)):
                return int(text)
            if isinstance(text, str):
                # Remover espacios y caracteres no numéricos excepto comas y puntos
                cleaned = re.sub(r'[^\d,.]', '', text.strip())
                if cleaned:
                    # Remover comas y convertir a entero
                    return int(cleaned.replace(',', '').replace('.', ''))
            return 0
        except:
            return 0
    
    def parse_currency(self, text):
        """Convierte texto de moneda a número"""
        try:
            if isinstance(text, (int, float)):
                return float(text)
            if isinstance(text, str):
                # Buscar patrones como "$ 74060000" o "ARS 55000"
                match = re.search(r'(?:ARS|[$])\s*([\d,]+)', text.strip())
                if match:
                    return float(match.group(1).replace(',', ''))
                # Si solo contiene números
                match = re.search(r'(\d+)', text.strip())
                if match:
                    return float(match.group(1))
            return 0.0
        except:
            return 0.0
    
    def parse_percentage(self, text):
        """Convierte texto de porcentaje a número"""
        try:
            if isinstance(text, (int, float)):
                return float(text)
            if isinstance(text, str):
                # Buscar patrones como "59,8%" o "59.8%"
                match = re.search(r'(\d+[,.]?\d*)%?', text.strip())
                if match:
                    return float(match.group(1).replace(',', '.'))
            return 0.0
        except:
            return 0.0
    
    def save_data_to_database(self, data):
        """Guarda los datos extraídos en la base de datos"""
        try:
            if not self.db_connection:
                logger.error("❌ No hay conexión a la base de datos")
                return False

            # Ajustar zona horaria (restar 3 horas para Argentina)
            fecha_extraccion = datetime.now() - timedelta(hours=3)
            cursor = self.db_connection.cursor()
            raw_data_ids = []

            # Procesar cada evento individualmente
            if "events_data" in data and data["events_data"]:
                logger.info(f"🎯 Procesando {len(data['events_data'])} eventos individualmente...")

                for event in data["events_data"]:
                    try:
                        # Extraer solo el nombre del artista del nombre del evento
                        nombre_completo = event.get("nombre_evento", "EVENTO_TICKETNORTE")
                        # Extraer solo la primera parte antes del primer " - " o " EN "
                        if " - " in nombre_completo:
                            artista = nombre_completo.split(" - ")[0].strip()
                        elif " EN " in nombre_completo:
                            artista = nombre_completo.split(" EN ")[0].strip()
                        else:
                            artista = nombre_completo

                        # Crear JSON individual para cada evento
                        event_json = {
                            "artista": artista,
                            "venue": event.get("recinto", "TicketNorte Venue"),
                            "fecha_show": event.get("fecha_evento", ""),
                            "event_id": event.get("nombre_evento", "").replace(" ", "_").lower(),
                            "event_url": event.get("borderaux_url", ""),
                            "categorias": event.get("categorias", []),
                            "resumen_total": event.get("resumen_total", {}),
                            "extraction_time": data.get("extraction_time", ""),
                            "username_used": data.get("username_used", ""),
                            "url": data.get("url", "")
                        }

                        # Calcular totales numéricos
                        total_vendido = event.get("resumen_total", {}).get("total_cantidad_vendida", 0)
                        total_recaudado = event.get("resumen_total", {}).get("total_general", 0)
                        total_cupos = event.get("resumen_total", {}).get("total_cupos", 0)
                        
                        # Calcular porcentaje de ocupación
                        porcentaje_ocupacion = 0
                        if total_cupos > 0:
                            porcentaje_ocupacion = (total_vendido / total_cupos) * 100

                        # Agregar totales calculados al JSON
                        event_json["totales"] = {
                            "total_vendido": total_vendido,
                            "total_recaudado": total_recaudado,
                            "total_capacidad": total_cupos,
                            "porcentaje_ocupacion": porcentaje_ocupacion
                        }

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

                        archivo_origen = f"ticketnorte_{event.get('nombre_evento', 'evento').replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        url_origen = event.get("borderaux_url", self.base_url)
                        json_data_str = json.dumps(event_json, ensure_ascii=False)

                        # Convertir fecha_show a timestamp
                        fecha_show_timestamp = None
                        if event.get("fecha_evento"):
                            try:
                                # Parsear fecha en formato DD/MM/YY HH:MM (año de 2 dígitos con hora)
                                fecha_str = event.get("fecha_evento")
                                # Separar fecha y hora
                                if " " in fecha_str:
                                    fecha_part, hora_part = fecha_str.split(" ", 1)
                                else:
                                    fecha_part = fecha_str
                                
                                # Parsear solo la parte de la fecha
                                fecha_parts = fecha_part.split("/")
                                if len(fecha_parts) == 3:
                                    day, month, year = fecha_parts
                                    # Convertir año de 2 dígitos a 4 dígitos
                                    year_4_digits = int(year)
                                    if year_4_digits < 100:
                                        year_4_digits += 2000
                                    fecha_show_timestamp = datetime(year_4_digits, int(month), int(day))
                            except Exception as e:
                                logger.warning(f"Error parseando fecha {event.get('fecha_evento')}: {e}")
                                fecha_show_timestamp = None

                        cursor.execute(insert_query, (
                            'ticketnorte',
                            json_data_str,
                            fecha_extraccion,
                            archivo_origen,
                            url_origen,
                            False,  # No procesado aún, el trigger lo procesará
                            artista,  # Usar la variable artista extraída
                            event_json['venue'],
                            fecha_show_timestamp
                        ))

                        raw_data_id = cursor.fetchone()[0]
                        raw_data_ids.append(raw_data_id)

                        logger.info(f"✅ Evento '{event.get('nombre_evento', 'Sin título')}' guardado con ID: {raw_data_id}")
                        logger.info(f"   📊 Tickets: {total_vendido}, Recaudado: ${total_recaudado:,}, Capacidad: {total_cupos}")

                    except Exception as e:
                        logger.error(f"❌ Error guardando evento '{event.get('nombre_evento', 'Sin título')}': {str(e)}")
                        continue

                self.db_connection.commit()
                cursor.close()

                logger.info(f"✅ {len(raw_data_ids)} eventos guardados exitosamente en base de datos")
                logger.info(f"📊 Usuario utilizado: {self.final_data['username_used']}")

                # Mostrar resumen de datos extraídos
                logger.info("=== RESUMEN DE DATOS EXTRAÍDOS ===")
                logger.info(f"EVENTOS PROCESADOS: {len(raw_data_ids)}")
                for i, event in enumerate(data["events_data"]):
                    logger.info(f"  {i+1}. {event.get('nombre_evento', 'Sin título')}")
                    logger.info(f"     Fecha: {event.get('fecha_evento', 'N/A')}")
                    logger.info(f"     Venue: {event.get('recinto', 'N/A')}")
                    logger.info(f"     Tickets vendidos: {event.get('resumen_total', {}).get('total_cantidad_vendida', 'N/A')}")
                    logger.info(f"     Total recaudado: ${event.get('resumen_total', {}).get('total_general', 'N/A')}")

                return raw_data_ids
            else:
                logger.warning("⚠️ No hay events_data para procesar")
                return []

        except Exception as e:
            logger.error(f"❌ Error guardando datos en base de datos: {str(e)}")
            if self.db_connection:
                self.db_connection.rollback()
            return None
    
    def volver_a_eventos(self):
        """Vuelve a la página de eventos en curso"""
        try:
            logger.info("Volviendo a la página de eventos en curso...")
            
            # Intentar volver usando el botón de navegación o URL directa
            self.driver.get("https://norteticket.com/administracion/")
            
            # Esperar a que cargue la página
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "eventos_en_curso"))
            )
            
            logger.info("✅ Vuelto exitosamente a la página de eventos")
            return True
            
        except Exception as e:
            logger.error(f"Error volviendo a eventos: {str(e)}")
            return False
    
    def run(self):
        """Ejecuta el proceso completo del scraper - VERSIÓN MEJORADA CON VENTAS DIARIAS"""
        try:
            logger.info("=== INICIANDO SCRAPER DE TICKETNORTE MEJORADO ===")
            print("🚀 SCRAPER NORTETICKET - VERSIÓN CON VENTAS DIARIAS")
            print("=" * 60)
            
            # Configurar driver
            if not self.setup_driver():
                logger.error("❌ No se pudo configurar el driver")
                return False
            
            # Navegar a la página de login
            if not self.navigate_to_login():
                logger.error("❌ No se pudo navegar a la página de login")
                return False
            
            # Realizar login
            username = "daleplay"
            password = "daleplay2025"
            
            if not self.login(username, password):
                logger.error("❌ Login fallido")
                return False
            
            logger.info("✅ Login exitoso")
            print("✅ Autenticación completada")
            
            # NUEVA FUNCIONALIDAD: Actualizar ventas diarias desde el endpoint
            if self.test_mode:
                logger.info("🧪 MODO TEST: Solo navegación y exploración del 'ojito' (sin BD)")
                print("\n🧪 EJECUTANDO EN MODO TEST (exploración del ojito)")
                
                try:
                    # Ir a grilla y listar eventos
                    self.driver.get("https://norteticket.com/administracion/")
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "eventos_en_curso"))
                    )
                    eventos_tabla = self.get_eventos_en_curso()
                    print(f"\n📋 Eventos visibles en tabla: {len(eventos_tabla)}")
                    for e in eventos_tabla[:5]:
                        print(f" - {e['nombre']} | {e['fecha']} | {e['ciudad']}")

                    if eventos_tabla:
                        for idx, ev in enumerate(eventos_tabla, 1):
                            print(f"\n➡️ Evento {idx}/{len(eventos_tabla)}: {ev['nombre']} | {ev['fecha']} | {ev['ciudad']}")
                            abierto = self.click_ojito(ev)
                            if not abierto:
                                # Fallback: clic directo global por href
                                abierto = self.click_first_ojito_global()
                            if abierto:
                                # Extraer sólo las dos métricas pedidas desde el DOM
                                self.inspect_ojito_page()
                                # Intentar match con BD por artista + fecha (solo test, sin guardar)
                                show_id = self.match_show_by_artist_and_date(ev['nombre'], ev['fecha'])
                                if show_id:
                                    print(f"🎯 MATCH BD show_id: {show_id}")
                                else:
                                    print("⚠️ Sin match en BD por artista+fecha")
                                # No guardar; solo inspección
                                self.volver_a_eventos()
                                # Reobtener tabla por si DataTables recarga
                                WebDriverWait(self.driver, 10).until(
                                    EC.presence_of_element_located((By.ID, "eventos_en_curso"))
                                )
                                try:
                                    WebDriverWait(self.driver, 5).until(
                                        EC.invisibility_of_element_located((By.ID, "eventos_en_curso_processing"))
                                    )
                                except Exception:
                                    pass
                                eventos_tabla = self.get_eventos_en_curso()
                            else:
                                print("❌ No se pudo abrir el ojito para este evento")
                    else:
                        print("❌ No hay eventos en la tabla para abrir el ojito")
                except Exception as e:
                    logger.warning(f"⚠️ Falló la exploración del ojito: {e}")
            else:
                logger.info("💾 MODO PRODUCCIÓN: Ingestando ventas diarias en BD desde Resumen Diario")
                print("\n💾 EJECUTANDO EN MODO PRODUCCIÓN (ingesta daily_sales)")
                try:
                    # Ir a grilla y listar eventos
                    self.driver.get("https://norteticket.com/administracion/")
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.ID, "eventos_en_curso"))
                    )
                    eventos_tabla = self.get_eventos_en_curso()
                    print(f"\n📋 Eventos visibles en tabla: {len(eventos_tabla)}")
                    for e in eventos_tabla[:5]:
                        print(f" - {e['nombre']} | {e['fecha']} | {e['ciudad']}")

                    if eventos_tabla:
                        for idx, ev in enumerate(eventos_tabla, 1):
                            print(f"\n➡️ Ingestando {idx}/{len(eventos_tabla)}: {ev['nombre']} | {ev['fecha']} | {ev['ciudad']}")
                            self.ingest_event_daily_sales(ev, test_only=False)
                    else:
                        print("❌ No hay eventos en la tabla para ingresar daily_sales")
                except Exception as e:
                    logger.error(f"❌ Error en ingesta de producción: {e}")
                    return False
            
            logger.info("=== SCRAPER COMPLETADO EXITOSAMENTE ===")
            print("\n✅ PROCESO COMPLETADO EXITOSAMENTE")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inesperado en el proceso: {str(e)}")
            print(f"❌ Error general: {e}")
            return False

    def dump_current_html(self):
        """Imprime y guarda todo el HTML actual de la página (post-ojito)."""
        try:
            current_url = self.driver.current_url
            html = self.driver.page_source
            # Detectar event_id si existe
            event_id = None
            m = re.search(r"/administracion/evento/(\d+)", current_url)
            if m:
                event_id = m.group(1)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"html_ojito_{event_id or 'sin_id'}_{ts}.html"
            # Guardar en el directorio actual de trabajo
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"\n===== DUMP HTML (event_id={event_id or 'N/A'}) =====")
            print(html)
            print(f"===== FIN DUMP HTML. Guardado en: {filename} =====\n")
            return True
        except Exception as e:
            logger.warning(f"No se pudo volcar el HTML: {e}")
            return False

def main():
    """Función principal para ejecutar el scraper en MODO PRODUCCIÓN"""
    scraper = None
    
    try:
        logger.info("🚀 INICIANDO NORTETICKET EN MODO PRODUCCIÓN")
        # Inicializar scraper para producción (con BD)
        scraper = NorteTicketScraper(headless=True, test_mode=False)
        
        # Ejecutar el proceso completo
        success = scraper.run()
        
        if success:
            logger.info("🎉 Scraper completado exitosamente")
            return True
        else:
            logger.error("❌ Scraper falló")
            return False
        
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        return False
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return False
    finally:
        if scraper:
            scraper.close()

def main_test():
    """Función principal para ejecutar el scraper en MODO TEST"""
    scraper = None
    
    try:
        logger.info("🧪 INICIANDO NORTETICKET EN MODO TEST")
        # Inicializar scraper para test (sin BD)
        scraper = NorteTicketScraper(headless=True, test_mode=True)
        
        # Ejecutar el proceso completo
        success = scraper.run()
        
        if success:
            logger.info("🎉 Test completado exitosamente")
            return True
        else:
            logger.error("❌ Test falló")
            return False
        
    except KeyboardInterrupt:
        logger.info("Test interrumpido por el usuario")
        return False
    except Exception as e:
        logger.error(f"Error inesperado en test: {str(e)}")
        return False
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    # Ejecutar en modo producción para actualizar BD
    main()
