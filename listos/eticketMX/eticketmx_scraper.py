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
import psycopg2
from psycopg2.extras import RealDictCursor
from database_config import get_database_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETicketMXScraper:
    def __init__(self, headless=False):
        """
        Inicializa el scraper de E-Ticket México optimizado para base de datos
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
        """
        self.driver = None
        self.headless = headless
        
        # Configuración para contenedores (no crear carpetas físicas)
        self.download_folder = "/tmp"  # Usar /tmp en contenedores
        
        # Credenciales de login
        self.username = "frsierra"
        self.password = "etk123"
        self.login_url = "http://clientes.eticket.com.mx/"
        
        # Inicializar conexión a base de datos
        self.db_connection = None
        self.db_connected = False
        self.setup_database_connection()
        
        # Datos finales para retornar (sin archivos físicos)
        self.final_data = {
            "ticketera": "eticketMX",
            "fecha_extraccion": None,
            "total_eventos_procesados": 0,
            "eventos_exitosos": 0,
            "eventos_con_error": 0,
            "datos_por_evento": {}
        }
    
    def setup_database_connection(self):
        """Verifica conexión con la base de datos PostgreSQL"""
        try:
            logger.info("🔌 Verificando conexión con la base de datos...")
            connection = get_database_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute("SELECT NOW();")
                result = cursor.fetchone()
                logger.info(f"✅ Conexión exitosa! Hora actual: {result[0]}")
                cursor.close()
                connection.close()
                self.db_connected = True
                return True
            else:
                logger.warning("⚠️ No se pudo establecer conexión a la base de datos")
                self.db_connected = False
                return False
        except Exception as e:
            logger.error(f"❌ Error en la conexión a la base de datos: {e}")
            self.db_connected = False
            return False
    
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
        """Realiza el login en el sistema E-Ticket México"""
        try:
            logger.info("Iniciando proceso de login...")
            
            # Navegar a la página de login
            logger.info(f"Navegando a: {self.login_url}")
            self.driver.get(self.login_url)
            time.sleep(3)
            
            # Buscar el campo de usuario
            try:
                user_field = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "Username"))
                )
                user_field.clear()
                user_field.send_keys(self.username)
                logger.info(f"Usuario ingresado: {self.username}")
            except:
                logger.error("No se encontró el campo de usuario")
                return False
            
            # Buscar el campo de contraseña
            try:
                password_field = self.driver.find_element(By.ID, "Password")
                password_field.clear()
                password_field.send_keys(self.password)
                logger.info("Contraseña ingresada")
            except:
                logger.error("No se encontró el campo de contraseña")
                return False
            
            # Hacer clic en el botón "Iniciar Sesión"
            try:
                login_button = self.driver.find_element(By.ID, "signIn")
                login_button.click()
                logger.info("Botón 'Iniciar Sesión' clickeado")
                time.sleep(5)
            except:
                logger.error("No se encontró el botón 'Iniciar Sesión'")
                return False
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            logger.info(f"URL actual después del login: {current_url}")
            
            # Si no estamos más en la página de login, el login fue exitoso
            if "login" not in current_url.lower() and self.login_url not in current_url:
                logger.info("Login exitoso")
                return True
            else:
                logger.warning("El login puede no haber sido exitoso")
                return False
                
        except Exception as e:
            logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def navigate_to_mis_eventos(self):
        """Navega al menú Reportes > Mis Eventos"""
        try:
            logger.info("Navegando a Reportes > Mis Eventos...")
            
            # Buscar el menú de Reportes
            try:
                reportes_menu = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Reportes')]"))
                )
                reportes_menu.click()
                logger.info("Menú 'Reportes' clickeado")
                time.sleep(2)
            except:
                logger.error("No se encontró el menú 'Reportes'")
                return False
            
            # Buscar y hacer clic en "Mis Eventos"
            try:
                mis_eventos_link = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[@href='/report/myevents']"))
                )
                mis_eventos_link.click()
                logger.info("Enlace 'Mis Eventos' clickeado")
                time.sleep(3)
            except:
                logger.error("No se encontró el enlace 'Mis Eventos'")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error navegando a Mis Eventos: {str(e)}")
            return False
    
    def navigate_to_ventas_diarias(self):
        """Navega directamente a la página de Venta por Día"""
        try:
            logger.info("Navegando directamente a Ventas Diarias...")
            
            # Navegar directamente a la URL de ventas diarias
            ventas_diarias_url = "https://clientes.eticket.com.mx/report/perday"
            self.driver.get(ventas_diarias_url)
            time.sleep(5)  # Esperar a que cargue la página
            
            # Verificar que estamos en la página correcta
            current_url = self.driver.current_url
            if "perday" in current_url:
                logger.info("✅ Navegación exitosa a Ventas Diarias")
                return True
            else:
                logger.error(f"❌ No se pudo navegar a Ventas Diarias. URL actual: {current_url}")
                return False
            
        except Exception as e:
            logger.error(f"Error navegando a Venta por Día: {str(e)}")
            return False
    
    def get_all_artists(self):
        """Obtiene todos los artistas disponibles del selector"""
        try:
            logger.info("Obteniendo lista de artistas...")
            
            # Buscar el select de artistas
            try:
                artist_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "PerformerId"))
                )
                logger.info("Selector de artistas encontrado")
            except:
                logger.error("No se encontró el selector de artistas")
                return []
            
            # Obtener todas las opciones
            select = Select(artist_select)
            options = select.options
            
            artists = []
            for option in options:
                artist_value = option.get_attribute("value")
                artist_text = option.text.strip()
                
                # Saltar la opción vacía
                if artist_value and artist_text and "SELECCIONE" not in artist_text.upper():
                    artists.append({
                        "value": artist_value,
                        "text": artist_text
                    })
            
            logger.info(f"Encontrados {len(artists)} artistas")
            return artists
            
        except Exception as e:
            logger.error(f"Error obteniendo artistas: {str(e)}")
            return []
    
    def select_artist_and_get_events(self, artist_value, artist_text):
        """Selecciona un artista y obtiene sus eventos"""
        try:
            logger.info(f"Seleccionando artista: {artist_text}")
            
            # Seleccionar el artista
            try:
                artist_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "PerformerId"))
                )
                select = Select(artist_select)
                select.select_by_value(artist_value)
                logger.info(f"Artista seleccionado: {artist_value}")
                time.sleep(3)  # Esperar a que se carguen los eventos
            except:
                logger.error(f"No se pudo seleccionar el artista {artist_value}")
                return []
            
            # Obtener eventos del artista
            try:
                event_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "EventId"))
                )
                
                select = Select(event_select)
                options = select.options
                
                events = []
                for option in options:
                    event_value = option.get_attribute("value")
                    event_text = option.text.strip()
                    
                    # Saltar la opción vacía
                    if event_value and event_text and "SELECCIONE" not in event_text.upper():
                        events.append({
                            "value": event_value,
                            "text": event_text,
                            "artist": artist_text
                        })
                
                logger.info(f"Encontrados {len(events)} eventos para {artist_text}")
                return events
                
            except:
                logger.warning(f"No se encontraron eventos para el artista {artist_text}")
                return []
            
        except Exception as e:
            logger.error(f"Error procesando artista {artist_text}: {str(e)}")
            return []
    
    def select_event_and_get_report(self, event_value, event_text, artist_text):
        """Selecciona un evento y obtiene el reporte"""
        try:
            logger.info(f"Procesando evento: {event_text}")
            
            # Seleccionar el evento
            try:
                event_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "EventId"))
                )
                select = Select(event_select)
                select.select_by_value(event_value)
                logger.info(f"Evento seleccionado: {event_value}")
                time.sleep(2)
            except:
                logger.error(f"No se pudo seleccionar el evento {event_value}")
                return None
            
            # Hacer clic en "RESUMEN"
            try:
                resumen_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "btn-2"))
                )
                resumen_button.click()
                logger.info("Botón 'RESUMEN' clickeado")
                time.sleep(5)  # Esperar a que se cargue el reporte
            except:
                logger.error("No se encontró el botón 'RESUMEN'")
                return None
            
            # Extraer datos del reporte
            return self.extract_report_data(event_value, event_text, artist_text)
            
        except Exception as e:
            logger.error(f"Error procesando evento {event_text}: {str(e)}")
            return None
    
    def extract_report_data(self, event_value, event_text, artist_text):
        """Extrae todos los datos del reporte de resumen"""
        try:
            logger.info("Extrayendo datos del reporte...")
            
            # Esperar a que se cargue el contenido dinámico
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "rpt-dinamic"))
                )
                logger.info("Reporte dinámico cargado")
            except:
                logger.warning("No se encontró el contenedor del reporte dinámico")
                return None
            
            extracted_data = {
                "evento_codigo": event_value,
                "evento_nombre": event_text,
                "artista": artist_text,
                "fecha_extraccion": datetime.now().isoformat(),
                "informacion_evento": {},
                "precios_preventa": [],
                "precios_dia_evento": [],
                "resumen_boletos_vendidos": [],
                "resumen_boletos_disponibles": [],
                "resumen_aforo": [],
                "cantidades_boletos": []
            }
            
            # Extraer información básica del evento
            extracted_data["informacion_evento"] = self.extract_event_info()
            
            # Verificar si es un evento futuro
            if not self.is_future_event(extracted_data["informacion_evento"]):
                logger.info(f"Evento {event_text} no es futuro, se omite")
                return None
            
            # Extraer todas las tablas
            extracted_data["precios_preventa"] = self.extract_table_by_title("PRECIOS DE PREVENTA")
            extracted_data["precios_dia_evento"] = self.extract_table_by_title("PRECIOS DÍA DEL EVENTO")
            extracted_data["resumen_boletos_vendidos"] = self.extract_table_by_title("RESUMEN DE BOLETOS VENDIDOS")
            extracted_data["resumen_boletos_disponibles"] = self.extract_table_by_title("RESUMEN DE BOLETOS DISPONIBLES")
            extracted_data["resumen_aforo"] = self.extract_table_by_title("RESUMEN DE AFORO")
            extracted_data["cantidades_boletos"] = self.extract_table_by_title("CANTIDADES DE BOLETOS")
            
            logger.info(f"Datos extraídos para evento: {event_text}")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos del reporte: {str(e)}")
            return None
    
    def extract_event_info(self):
        """Extrae la información básica del evento"""
        try:
            event_info = {}
            
            # Buscar todas las filas de información del evento
            try:
                info_rows = self.driver.find_elements(By.XPATH, "//table//tr[td[contains(@class, 'azulmarino')]]")
                
                for row in info_rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 2:
                        key = cells[0].text.strip().replace(":", "")
                        value = cells[1].text.strip()
                        
                        # Normalizar el nombre de la clave
                        key_normalized = key.lower().replace(" ", "_").replace("ñ", "n")
                        key_normalized = re.sub(r'[^\w]', '_', key_normalized)
                        key_normalized = re.sub(r'_+', '_', key_normalized).strip('_')
                        
                        event_info[key_normalized] = value
                        
            except:
                logger.warning("No se pudo extraer información del evento")
            
            return event_info
            
        except Exception as e:
            logger.error(f"Error extrayendo información del evento: {str(e)}")
            return {}
    
    def is_future_event(self, event_info):
        """Verifica si el evento es futuro o actual"""
        try:
            fecha_str = event_info.get("fecha", "")
            if not fecha_str:
                return True  # Si no hay fecha, incluir por defecto
            
            # Parsear la fecha (formato: "sábado, 27 de septiembre de 2025")
            # Extraer solo la parte de la fecha
            meses = {
                "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
                "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
                "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
            }
            
            parts = fecha_str.lower().replace(",", "").split()
            if len(parts) >= 4:
                day = int(parts[1])
                month_name = parts[3]
                year = int(parts[5])
                
                if month_name in meses:
                    month = meses[month_name]
                    event_date = datetime(year, month, day).date()
                    today = datetime.now().date()
                    
                    logger.info(f"Fecha del evento: {event_date}, Hoy: {today}, ¿Futuro? {event_date >= today}")
                    return event_date >= today
            
            return True  # Si no se puede parsear, incluir por defecto
            
        except Exception as e:
            logger.warning(f"Error parseando fecha del evento: {str(e)}")
            return True  # Si hay error, incluir por defecto
    
    def extract_table_by_title(self, title):
        """Extrae una tabla específica por su título"""
        try:
            # Buscar el título de la tabla
            title_element = self.driver.find_element(By.XPATH, f"//div[contains(@class, 'titulosprincipales') and contains(text(), '{title}')]")
            
            # Buscar la tabla que sigue al título
            table = title_element.find_element(By.XPATH, "following-sibling::div//table")
            
            return self.extract_table_data(table, title)
            
        except:
            logger.warning(f"No se encontró la tabla: {title}")
            return []
    
    def extract_table_data(self, table, table_name):
        """Extrae datos de una tabla específica"""
        try:
            rows_data = []
            
            # Extraer todas las filas
            rows = table.find_elements(By.TAG_NAME, "tr")
            headers = []
            
            for i, row in enumerate(rows):
                cells = row.find_elements(By.CSS_SELECTOR, "td, th")
                
                if not cells:
                    continue
                
                # La primera fila con contenido son los headers
                if i == 0 or not headers:
                    headers = [cell.text.strip() for cell in cells]
                    continue
                
                # Procesar filas de datos
                if len(cells) > 0:
                    row_data = {
                        "tabla": table_name,
                        "fecha_extraccion": datetime.now().isoformat()
                    }
                    
                    # Mapear cada celda con su header correspondiente
                    for j, cell in enumerate(cells):
                        header_name = headers[j] if j < len(headers) else f"Columna_{j+1}"
                        cell_text = cell.text.strip()
                        
                        # Normalizar el nombre del header
                        header_key = header_name.lower().replace(" ", "_").replace("ñ", "n")
                        header_key = re.sub(r'[^\w]', '_', header_key)
                        header_key = re.sub(r'_+', '_', header_key).strip('_')
                        
                        row_data[header_key] = cell_text if cell_text else ""
                    
                    rows_data.append(row_data)
            
            logger.info(f"Tabla '{table_name}': {len(rows_data)} filas extraídas")
            return rows_data
            
        except Exception as e:
            logger.error(f"Error extrayendo tabla '{table_name}': {str(e)}")
            return []
    
    def save_data_to_database(self, all_data):
        """Guarda los datos extraídos en la base de datos y procesa automáticamente"""
        try:
            if not all_data:
                logger.warning("No hay datos para guardar")
                return False
            
            logger.info(f"💾 Guardando {len(all_data)} eventos en la base de datos...")
            
            successful_saves = 0
            failed_saves = 0
            
            # Procesar cada evento individualmente
            for evento_data in all_data:
                try:
                    success = self.save_single_event_to_database(evento_data)
                    if success:
                        successful_saves += 1
                    else:
                        failed_saves += 1
                except Exception as e:
                    logger.error(f"Error guardando evento individual: {str(e)}")
                    failed_saves += 1
            
            logger.info(f"✅ Guardado completado: {successful_saves} exitosos, {failed_saves} fallidos")
            
            # Actualizar datos finales
            self.final_data["total_eventos_procesados"] = len(all_data)
            self.final_data["eventos_exitosos"] = successful_saves
            self.final_data["eventos_con_error"] = failed_saves
            self.final_data["fecha_extraccion"] = (datetime.now() - timedelta(hours=3)).isoformat()
            
            return successful_saves > 0
            
        except Exception as e:
            logger.error(f"Error guardando datos en base de datos: {str(e)}")
            return False
    
    def process_complete_flow(self, all_data):
        """Procesa el flujo completo: raw_data → shows → daily_sales"""
        try:
            if not all_data:
                logger.warning("No hay datos para procesar")
                return False
            
            logger.info(f"🔄 Procesando flujo completo para {len(all_data)} eventos...")
            
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return False
            
            cursor = connection.cursor()
            processed_count = 0
            
            for evento_data in all_data:
                try:
                    success = self.process_single_event_complete(cursor, evento_data)
                    if success:
                        processed_count += 1
                        logger.info(f"✅ Evento procesado: {evento_data.get('evento_nombre', '')}")
                    else:
                        logger.warning(f"⚠️ Error procesando: {evento_data.get('evento_nombre', '')}")
                        
                except Exception as e:
                    logger.error(f"❌ Error procesando evento {evento_data.get('evento_nombre', '')}: {str(e)}")
                    continue
            
            # Commit y cerrar conexión
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"🎉 Procesamiento completado: {processed_count}/{len(all_data)} eventos procesados exitosamente")
            return processed_count > 0
            
        except Exception as e:
            logger.error(f"❌ Error procesando flujo completo: {str(e)}")
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'connection' in locals() and connection:
                    connection.rollback()
                    connection.close()
            except:
                pass
            return False
    
    def process_single_event_complete(self, cursor, evento_data):
        """Procesa un evento completo: raw_data → shows → daily_sales"""
        try:
            artista = evento_data.get('artista', '')
            evento_nombre = evento_data.get('evento_nombre', '')
            
            if not artista or not evento_nombre:
                logger.warning("⚠️ Evento sin artista o nombre, saltando...")
                return False
            
            # Parsear fecha del evento
            informacion_evento = evento_data.get('informacion_evento', {})
            fecha_str = informacion_evento.get('fecha', '')
            
            if not fecha_str:
                # Intentar extraer del nombre del evento
                import re
                fecha_match = re.search(r'\((\d{2}-\d{2}-\d{4})', evento_nombre)
                if fecha_match:
                    fecha_str = fecha_match.group(1)
                    # Convertir formato DD-MM-YYYY a YYYY-MM-DD
                    fecha_parts = fecha_str.split('-')
                    if len(fecha_parts) == 3:
                        fecha_str = f"{fecha_parts[2]}-{fecha_parts[1]}-{fecha_parts[0]}"
            
            # Parsear fecha en formato español
            if fecha_str and "de" in fecha_str:
                import re
                meses = {
                    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
                    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
                    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
                }
                
                fecha_match = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', fecha_str)
                if fecha_match:
                    dia = fecha_match.group(1).zfill(2)
                    mes_nombre = fecha_match.group(2).lower()
                    año = fecha_match.group(3)
                    
                    if mes_nombre in meses:
                        mes = meses[mes_nombre]
                        fecha_str = f"{año}-{mes}-{dia}"
            
            if not fecha_str or len(fecha_str) != 10:
                logger.warning(f"⚠️ No se pudo parsear fecha del evento: {fecha_str}")
                return False
            
            # Extraer ciudad del nombre del evento
            ciudad = ""
            if ":" in evento_nombre:
                ciudad = evento_nombre.split(":")[0].strip()
            
            # Calcular totales del evento
            totales_evento = self.calculate_event_totals(evento_data)
            
            # Obtener fecha de extracción
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            # 1. GUARDAR EN RAW_DATA
            raw_data_id = self.save_raw_data_eticketmx(cursor, artista, fecha_str, evento_data, fecha_extraccion_utc3)
            
            # 2. CREAR/ACTUALIZAR SHOW
            show_id, vendido_anterior, recaudacion_anterior = self.create_or_update_show_eticketmx(cursor, artista, fecha_str, totales_evento, ciudad)
            
            # 3. PROCESAR DAILY_SALES (si tenemos datos de ventas diarias)
            if 'daily_sales' in evento_data and evento_data['daily_sales']:
                self.process_daily_sales_eticketmx(cursor, show_id, artista, fecha_str, totales_evento, fecha_extraccion_utc3, vendido_anterior, recaudacion_anterior, evento_data['daily_sales'])
            
            logger.info(f"✅ Procesamiento completo de '{artista}' - {fecha_str} exitoso")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error procesando evento completo: {str(e)}")
            return False
    
    def update_daily_sales_for_show(self, artista, venue, daily_sales_data, fecha_evento=None):
        """Actualiza las ventas diarias para un show específico"""
        try:
            if not self.db_connected:
                logger.warning("⚠️ Base de datos no conectada, no se pueden actualizar datos")
                return False
            
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return False
            
            cursor = connection.cursor()
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            # Usar la fecha del evento pasada como parámetro
            if not fecha_evento:
                logger.warning(f"⚠️ No se proporcionó fecha del evento para {artista}")
                cursor.close()
                connection.close()
                return False
            
            # Convertir fecha_evento a datetime para comparación
            fecha_evento_dt = datetime.strptime(fecha_evento, "%Y-%m-%d").date()
            
            # Buscar show por artista, ticketera y fecha exacta
            cursor.execute("""
                SELECT id, fecha_show FROM shows 
                WHERE artista = %s AND ticketera = 'eticketMX' AND fecha_show IS NOT NULL
                AND DATE(fecha_show) = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (artista, fecha_evento_dt))
            
            show_result = cursor.fetchone()
            if not show_result:
                logger.warning(f"⚠️ No se encontró show válido para {artista} con fecha alrededor de {fecha_evento}")
                cursor.close()
                connection.close()
                return False
            
            show_id = show_result[0]
            fecha_show_real = show_result[1]
            logger.info(f"📊 Actualizando ventas diarias para show_id: {show_id} ({artista} - {fecha_show_real})")
            
            # Verificar qué fechas ya existen en la base de datos
            cursor.execute("""
                SELECT fecha_venta
                FROM daily_sales 
                WHERE show_id = %s 
                ORDER BY fecha_venta DESC
            """, (show_id,))
            
            fechas_existentes = {row[0] for row in cursor.fetchall()}
            fecha_mas_reciente_bd = max(fechas_existentes) if fechas_existentes else None
            
            logger.info(f"📅 Fechas existentes en BD: {len(fechas_existentes)} fechas")
            if fecha_mas_reciente_bd:
                logger.info(f"📅 Fecha más reciente en BD: {fecha_mas_reciente_bd}")
            
            # Actualizar el país a México para eticketMX
            cursor.execute("""
                UPDATE shows SET pais = 'México' 
                WHERE id = %s AND ticketera = 'eticketMX'
            """, (show_id,))
            
            # Obtener totales del evento (para capacidad total)
            totales = daily_sales_data.get('totales', {})
            capacidad_total = totales.get('boletos_total', 0)
            
            # Calcular totales acumulados basados en ventas diarias reales
            ventas_diarias = daily_sales_data.get('daily_sales', [])
            total_vendido_acumulado = sum(venta.get('vendidos', 0) for venta in ventas_diarias)
            total_recaudacion_acumulada = sum(venta.get('importe', 0) for venta in ventas_diarias)
            
            totales_show = {
                'vendido_total': total_vendido_acumulado,
                'recaudacion_total_ars': total_recaudacion_acumulada,
                'disponible_total': capacidad_total - total_vendido_acumulado,
                'porcentaje_ocupacion': 0
            }
            
            # Calcular porcentaje de ocupación
            if capacidad_total > 0:
                totales_show['porcentaje_ocupacion'] = (total_vendido_acumulado / capacidad_total) * 100
            
            # Procesar cada fecha de venta diaria
            daily_sales = daily_sales_data.get('daily_sales', [])
            updated_count = 0
            created_count = 0
            skipped_count = 0
            
            # Ordenar ventas diarias por fecha para calcular acumulados correctamente
            daily_sales_sorted = sorted(daily_sales, key=lambda x: x.get('fecha_venta', ''))
            
            for i, venta_diaria in enumerate(daily_sales_sorted):
                try:
                    fecha_venta_str = venta_diaria.get('fecha_venta', '')
                    if not fecha_venta_str:
                        continue
                    
                    # Convertir string a date
                    fecha_venta = datetime.strptime(fecha_venta_str, "%Y-%m-%d").date()
                    
                    # Verificar si ya existe esta fecha
                    fecha_existe = fecha_venta in fechas_existentes
                    
                    # Solo actualizar si la fecha no existe en la BD
                    # (Por ahora, siempre actualizamos para recalcular los totales acumulados)
                    if fecha_existe:
                        # Por ahora, siempre actualizamos para recalcular totales acumulados
                        pass
                    
                    # Verificar si ya existe un registro para esta fecha
                    cursor.execute("""
                        SELECT id FROM daily_sales 
                        WHERE show_id = %s AND fecha_venta = %s
                    """, (show_id, fecha_venta))
                    
                    existing_record = cursor.fetchone()
                    
                    # Preparar datos para actualización/inserción (simplificado)
                    venta_diaria_count = venta_diaria.get('vendidos', 0)  # Solo ventas
                    monto_diario_ars = venta_diaria.get('importe', 0)  # Monto diario en pesos mexicanos
                    
                    # Calcular totales acumulados hasta esta fecha
                    ventas_hasta_fecha = daily_sales_sorted[:i+1]  # Incluir la fecha actual
                    venta_total_acumulada = sum(v.get('vendidos', 0) for v in ventas_hasta_fecha)
                    recaudacion_total_acumulada = sum(v.get('importe', 0) for v in ventas_hasta_fecha)
                    
                    if existing_record:
                        # Actualizar registro existente
                        cursor.execute("""
                            UPDATE daily_sales SET
                                venta_diaria = %s,
                                monto_diario_ars = %s,
                                venta_total_acumulada = %s,
                                recaudacion_total_ars = %s,
                                fecha_extraccion = %s,
                                updated_at = NOW()
                            WHERE show_id = %s AND fecha_venta = %s
                        """, (
                            venta_diaria_count,
                            monto_diario_ars,
                            venta_total_acumulada,
                            recaudacion_total_acumulada,
                            fecha_extraccion_utc3,
                            show_id,
                            fecha_venta
                        ))
                        updated_count += 1
                        logger.info(f"✅ Actualizado daily_sales para {fecha_venta}")
                    else:
                        # Insertar nuevo registro
                        cursor.execute("""
                            INSERT INTO daily_sales (
                                show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,
                                venta_total_acumulada, recaudacion_total_ars, archivo_origen, url_origen, ticketera
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """, (
                            show_id, fecha_venta, fecha_extraccion_utc3, venta_diaria_count, monto_diario_ars,
                            venta_total_acumulada, recaudacion_total_acumulada,
                            f"eticketmx_daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                            "https://clientes.eticket.com.mx/report/perday", "eticketMX"
                        ))
                        created_count += 1
                        logger.info(f"✅ Creado daily_sales para {fecha_venta}")
                    
                except Exception as e:
                    logger.error(f"❌ Error procesando venta diaria {venta_diaria}: {str(e)}")
                    continue
            
            # Commit y cerrar conexión
            connection.commit()
            cursor.close()
            connection.close()
            
            logger.info(f"✅ Procesadas {len(daily_sales)} fechas: {updated_count} actualizadas, {created_count} creadas, {skipped_count} omitidas para {artista} - {fecha_evento_dt}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error actualizando ventas diarias para {artista} - {venue}: {str(e)}")
            
            # Intentar cerrar conexión si está abierta
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'connection' in locals() and connection:
                    connection.close()
            except:
                pass
            
            return False

    def save_single_event_to_database(self, evento_data):
        """Guarda un evento individual en la base de datos (solo raw_data)"""
        try:
            if not self.db_connected:
                logger.warning("⚠️ Base de datos no conectada, no se pueden guardar datos")
                return False
            
            # Obtener conexión
            connection = get_database_connection()
            if not connection:
                logger.error("❌ No se pudo obtener conexión a la base de datos")
                return False
            
            cursor = connection.cursor()
            
            # Preparar datos para inserción
            fecha_extraccion_utc3 = datetime.now() - timedelta(hours=3)
            
            # Extraer información del evento
            evento_nombre = evento_data.get('evento_nombre', '')
            artista = evento_data.get('artista', '')
            
            # Extraer ciudad del nombre del evento (formato: "CIUDAD: ARTISTA - EVENTO")
            ciudad = ""
            if ":" in evento_nombre:
                ciudad = evento_nombre.split(":")[0].strip()
            
            # Extraer fecha del nombre del evento (formato: "CIUDAD: ARTISTA - EVENTO (DD-MM-YYYY HH:MM)")
            fecha_evento_parsed = "2025-01-01"  # Fecha por defecto
            import re
            fecha_match = re.search(r'\((\d{2}-\d{2}-\d{4})', evento_nombre)
            if fecha_match:
                fecha_str = fecha_match.group(1)
                # Convertir formato DD-MM-YYYY a YYYY-MM-DD
                fecha_parts = fecha_str.split('-')
                if len(fecha_parts) == 3:
                    fecha_evento_parsed = f"{fecha_parts[2]}-{fecha_parts[1]}-{fecha_parts[0]}"
            
            # Crear JSON individual para este evento con ventas diarias
            json_individual = {
                'evento': evento_nombre,
                'artista': artista,
                'venue': ciudad,
                'fecha_evento': fecha_evento_parsed,
                'ciudad': ciudad,
                'ventas_diarias': evento_data.get('daily_sales', {}),
                'fecha_extraccion': evento_data.get('fecha_extraccion', '')
            }
            
            # Preparar datos para inserción
            insert_data = {
                "ticketera": "eticketMX",
                "artista": artista,
                "venue": ciudad,
                "fecha_show": fecha_evento_parsed,
                "json_data": json.dumps(json_individual, ensure_ascii=False),
                "archivo_origen": f"eticketmx_daily_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "url_origen": "https://clientes.eticket.com.mx/report/perday",
                "fecha_extraccion": fecha_extraccion_utc3.isoformat(),
                "procesado": False
            }
            
            # Query de inserción
            insert_query = """
            INSERT INTO raw_data (
                ticketera, artista, venue, fecha_show, json_data, 
                archivo_origen, url_origen, fecha_extraccion, procesado
            ) VALUES (
                %(ticketera)s, %(artista)s, %(venue)s, %(fecha_show)s, %(json_data)s,
                %(archivo_origen)s, %(url_origen)s, %(fecha_extraccion)s, %(procesado)s
            ) RETURNING id;
            """
            
            # Ejecutar inserción
            cursor.execute(insert_query, insert_data)
            result = cursor.fetchone()
            
            if result:
                logger.info(f"✅ Datos de '{evento_nombre}' guardados exitosamente en raw_data (ID: {result[0]})")
                print(f"💾 GUARDADO EN RAW_DATA: {evento_nombre} - ID: {result[0]}")
                
                # Commit y cerrar conexión
                connection.commit()
                cursor.close()
                connection.close()
                
                # Ahora actualizar las ventas diarias
                daily_sales_data = evento_data.get('daily_sales', {})
                if daily_sales_data and daily_sales_data.get('daily_sales'):
                    logger.info(f"🔄 Actualizando ventas diarias para {artista} - {ciudad}")
                    self.update_daily_sales_for_show(artista, ciudad, daily_sales_data, fecha_evento_parsed)
                
                return True
            else:
                logger.warning(f"⚠️ Inserción completada pero sin ID retornado para '{evento_nombre}'")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos de '{evento_nombre}' en la BD: {str(e)}")
            print(f"❌ ERROR GUARDANDO EN BD: {evento_nombre} - {str(e)}")
            
            # Intentar cerrar conexión si está abierta
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'connection' in locals() and connection:
                    connection.rollback()
                    connection.close()
            except:
                pass
            
            return False
    
    def calculate_event_totals(self, evento_data):
        """Calcula los totales de un evento específico"""
        try:
            # En eticketMX, los totales están en cantidades_boletos y resumen_boletos_vendidos
            cantidades_boletos = evento_data.get("cantidades_boletos", [])
            resumen_vendidos = evento_data.get("resumen_boletos_vendidos", [])
            
            # Calcular totales sumando las filas de cantidades_boletos
            capacidad_total = 0
            vendido_total = 0
            disponible_total = 0
            recaudacion_total = 0
            
            # Sumar de cantidades_boletos (última fila tiene los totales)
            for fila in cantidades_boletos:
                if isinstance(fila, dict):
                    # La última fila tiene los totales (sin zona_bloque)
                    if not fila.get('zona_bloque', '').strip():
                        # Extraer totales de la fila de totales
                        cap = fila.get('cap', '0')
                        vnd = fila.get('vnd', '0')
                        lib = fila.get('lib', '0')
                        
                        # Convertir a números
                        try:
                            capacidad_total = int(cap) if cap.isdigit() else 0
                            vendido_total = int(vnd) if vnd.isdigit() else 0
                            disponible_total = int(lib) if lib.isdigit() else 0
                        except:
                            pass
                        break
            
            # Si no encontramos totales en cantidades_boletos, sumar de resumen_vendidos
            if capacidad_total == 0 and vendido_total == 0:
                for fila in resumen_vendidos:
                    if isinstance(fila, dict):
                        adulto = fila.get('adulto', '0')
                        try:
                            vendido_total += int(adulto) if adulto.isdigit() else 0
                        except:
                            pass
            
            # Calcular totales del evento
            totales_evento = {
                "capacidad_total": capacidad_total,
                "vendido_total": vendido_total,
                "disponible_total": disponible_total,
                "recaudacion_total_ars": recaudacion_total,
                "porcentaje_ocupacion": 0
            }
            
            # Calcular porcentaje de ocupación
            if totales_evento["capacidad_total"] > 0:
                totales_evento["porcentaje_ocupacion"] = round(
                    (totales_evento["vendido_total"] / totales_evento["capacidad_total"]) * 100, 2
                )
            
            logger.info(f"📊 Totales calculados para evento:")
            logger.info(f"  📊 Capacidad: {totales_evento['capacidad_total']}")
            logger.info(f"  🎫 Vendido: {totales_evento['vendido_total']}")
            logger.info(f"  🆓 Disponible: {totales_evento['disponible_total']}")
            logger.info(f"  💰 Recaudación: ${totales_evento['recaudacion_total_ars']:,}")
            logger.info(f"  📈 Ocupación: {totales_evento['porcentaje_ocupacion']}%")
            
            return totales_evento
            
        except Exception as e:
            logger.error(f"❌ Error calculando totales del evento: {str(e)}")
            return {
                "capacidad_total": 0,
                "vendido_total": 0,
                "disponible_total": 0,
                "recaudacion_total_ars": 0,
                "porcentaje_ocupacion": 0
            }
    
    def get_daily_sales_for_event(self, artist_value, artist_text, event_value, event_text):
        """Obtiene las ventas diarias para un evento específico"""
        try:
            logger.info(f"📅 Obteniendo ventas diarias para: {event_text}")
            
            # Seleccionar el artista
            try:
                artist_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "PerformerId"))
                )
                select = Select(artist_select)
                select.select_by_value(artist_value)
                logger.info(f"Artista seleccionado: {artist_value}")
                time.sleep(2)  # Esperar a que se carguen los eventos
            except:
                logger.error(f"No se pudo seleccionar el artista {artist_value}")
                return []
            
            # Seleccionar el evento
            try:
                event_select = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "EventId"))
                )
                select = Select(event_select)
                select.select_by_value(event_value)
                logger.info(f"Evento seleccionado: {event_value}")
                time.sleep(2)
            except:
                logger.error(f"No se pudo seleccionar el evento {event_value}")
                return []
            
            # Hacer clic en "Generar"
            try:
                generar_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "submit"))
                )
                generar_button.click()
                logger.info("Botón 'Generar' clickeado")
                time.sleep(15)  # Esperar 15 segundos a que se cargue el reporte
            except:
                logger.error("No se encontró el botón 'Generar'")
                return []
            
            # Extraer datos de la tabla de ventas diarias
            return self.extract_daily_sales_table(event_text)
            
        except Exception as e:
            logger.error(f"Error obteniendo ventas diarias para {event_text}: {str(e)}")
            return []
    
    def extract_daily_sales_table(self, event_text):
        """Extrae los datos de la tabla de ventas diarias"""
        try:
            logger.info("Extrayendo tabla de ventas diarias...")
            
            # Buscar la tabla de resultados
            try:
                table = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table') or contains(@class, 'report')]"))
                )
                logger.info("Tabla de ventas diarias encontrada")
            except:
                logger.warning("No se encontró la tabla de ventas diarias")
                return []
            
            daily_sales_data = []
            totales = {}
            rows = table.find_elements(By.TAG_NAME, "tr")
            
            # Procesar todas las filas
            for i, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:  # Fecha, Boletos, Vendidos, Cortesías, Importe
                        fecha_str = cells[0].text.strip()
                        boletos_str = cells[1].text.strip().replace(',', '')
                        vendidos_str = cells[2].text.strip().replace(',', '')
                        cortesias_str = cells[3].text.strip().replace(',', '')
                        importe_str = cells[4].text.strip().replace('$', '').replace(',', '')
                        
                        # Verificar si es la fila de totales
                        if fecha_str.upper() == "TOTAL":
                            totales = {
                                "boletos_total": int(boletos_str) if boletos_str.isdigit() else 0,
                                "vendidos_total": int(vendidos_str) if vendidos_str.isdigit() else 0,
                                "cortesias_total": int(cortesias_str) if cortesias_str.isdigit() else 0,
                                "importe_total": float(importe_str) if importe_str.replace('.', '').isdigit() else 0
                            }
                            logger.info(f"📊 Totales encontrados: {totales}")
                            continue
                        
                        # Parsear fecha (saltar si no es una fecha válida)
                        try:
                            fecha_venta = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                        except:
                            logger.warning(f"No se pudo parsear fecha: {fecha_str}")
                            continue
                        
                        # Convertir números
                        try:
                            boletos = int(boletos_str) if boletos_str.isdigit() else 0
                            vendidos = int(vendidos_str) if vendidos_str.isdigit() else 0
                            cortesias = int(cortesias_str) if cortesias_str.isdigit() else 0
                            importe = float(importe_str) if importe_str.replace('.', '').isdigit() else 0
                        except:
                            logger.warning(f"Error convirtiendo números para fecha {fecha_str}")
                            continue
                        
                        daily_sales_data.append({
                            "fecha_venta": fecha_venta.isoformat(),  # Convertir a string para JSON
                            "boletos": boletos,  # Total de boletos (vendidos + cortesías)
                            "vendidos": vendidos,  # Solo ventas
                            "cortesias": cortesias,  # Solo cortesías
                            "importe": importe,  # Monto diario
                            "evento": event_text
                        })
                        
                except Exception as e:
                    logger.warning(f"Error procesando fila {i}: {str(e)}")
                    continue
            
            # Agregar totales al resultado
            result = {
                "daily_sales": daily_sales_data,
                "totales": totales,
                "evento": event_text,
                "fecha_extraccion": datetime.now().isoformat()
            }
            
            logger.info(f"📊 Extraídas {len(daily_sales_data)} fechas de ventas diarias")
            return result
            
        except Exception as e:
            logger.error(f"Error extrayendo tabla de ventas diarias: {str(e)}")
            return []
    
    def save_raw_data_eticketmx(self, cursor, artist_name, fecha_show, evento_data, fecha_extraccion):
        """Guarda datos en raw_data"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archivo_origen = f"eticketmx_scraper_{timestamp}"
            
            # Crear JSON individual para este evento
            json_individual = {
                'evento': evento_data.get('evento_nombre', ''),
                'venue': evento_data.get('venue', ''),
                'fecha_evento': fecha_show,
                'ciudad': evento_data.get('venue', ''),
                'totales_evento': self.calculate_event_totals(evento_data),
                'sectores': evento_data.get('cantidades_boletos', []),
                'resumen_total': evento_data.get('resumen_boletos_vendidos', []),
                'daily_sales': evento_data.get('daily_sales', [])
            }
            
            cursor.execute("""
                INSERT INTO raw_data (ticketera, artista, venue, fecha_show, archivo_origen, fecha_extraccion, json_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                "eticketMX",
                artist_name,
                evento_data.get('venue', ''),
                fecha_show,
                archivo_origen,
                fecha_extraccion.isoformat(),
                json.dumps(json_individual, ensure_ascii=False)
            ))
            
            result = cursor.fetchone()
            if result:
                logger.info(f"✅ Raw data guardado (ID: {result[0]})")
                return result[0]
            else:
                logger.warning("⚠️ Raw data guardado pero sin ID retornado")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error guardando raw data: {str(e)}")
            return None
    
    def create_or_update_show_eticketmx(self, cursor, artist_name, fecha_show, totales=None, venue=None):
        """Busca show existente por artista y fecha, si no existe lo crea"""
        try:
            # Extraer solo la fecha (sin hora) para matching más flexible
            fecha_solo = fecha_show.split(' ')[0]  # YYYY-MM-DD
            
            # Normalizar nombre del artista para matching más robusto
            artist_name_normalized = artist_name.strip().upper()
            
            # Buscar show existente por artista normalizado, fecha (sin hora) y ticketera
            cursor.execute("""
                SELECT id, venta_total_acumulada, recaudacion_total_ars 
                FROM shows 
                WHERE UPPER(artista) = %s 
                AND DATE(fecha_show) = %s 
                AND ticketera = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (artist_name_normalized, fecha_solo, "eticketMX"))
            
            existing_show = cursor.fetchone()
            
            if existing_show:
                show_id, vendido_anterior, recaudacion_anterior = existing_show
                logger.info(f"📋 Show existente encontrado (ID: {show_id})")
                
                # Actualizar capacidad total si tenemos totales
                if totales and totales.get('capacidad_total', 0) > 0:
                    cursor.execute("""
                        UPDATE shows SET 
                            capacidad_total = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (totales['capacidad_total'], show_id))
                    logger.info(f"📊 Capacidad actualizada: {totales['capacidad_total']}")
                
                return show_id, vendido_anterior or 0, recaudacion_anterior or 0
            else:
                logger.info(f"🆕 Creando nuevo show para {artist_name} - {fecha_show}")
                
                # Crear nuevo show
                if totales:
                    cursor.execute("""
                        INSERT INTO shows (artista, venue, fecha_show, ticketera, estado, capacidad_total, ciudad)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        artist_name,
                        venue or '',
                        fecha_show,
                        "eticketMX",
                        "activo",
                        totales.get('capacidad_total', 0),
                        venue or ''
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO shows (artista, venue, fecha_show, ticketera, estado, ciudad)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        artist_name,
                        venue or '',
                        fecha_show,
                        "eticketMX",
                        "activo",
                        venue or ''
                    ))
                
                result = cursor.fetchone()
                if result:
                    show_id = result[0]
                    logger.info(f"✅ Nuevo show creado (ID: {show_id})")
                    return show_id, 0, 0
                else:
                    logger.error("❌ Error creando show")
                    return None, 0, 0
                    
        except Exception as e:
            logger.error(f"❌ Error en create_or_update_show: {str(e)}")
            return None, 0, 0
    
    def process_daily_sales_eticketmx(self, cursor, show_id, artist_name, fecha_show, totales_show, fecha_extraccion, vendido_anterior=0, recaudacion_anterior=0, daily_sales_data=None):
        """Procesa daily_sales con datos de ventas diarias reales"""
        try:
            if not show_id:
                logger.warning("⚠️ No hay show_id para procesar daily_sales")
                return
            
            # Si tenemos datos de ventas diarias, procesarlos
            if daily_sales_data and len(daily_sales_data) > 0:
                logger.info(f"📅 Procesando {len(daily_sales_data)} fechas de ventas diarias")
                
                for daily_data in daily_sales_data:
                    fecha_venta = daily_data.get('fecha_venta')
                    venta_diaria = daily_data.get('vendidos', 0)
                    monto_diario = daily_data.get('importe', 0)
                    
                    if not fecha_venta:
                        continue
                    
                    # Buscar si ya existe un registro para esta fecha
                    cursor.execute("""
                        SELECT id FROM daily_sales 
                        WHERE show_id = %s AND fecha_venta = %s
                    """, (show_id, fecha_venta))
                    
                    existing_record = cursor.fetchone()
                    
                    if existing_record:
                        # Actualizar registro existente
                        cursor.execute("""
                            UPDATE daily_sales SET
                                fecha_extraccion = %s,
                                venta_diaria = %s,
                                monto_diario_ars = %s,
                                updated_at = NOW()
                            WHERE show_id = %s AND fecha_venta = %s
                        """, (fecha_extraccion.isoformat(), venta_diaria, monto_diario, show_id, fecha_venta))
                        logger.info(f"📊 Daily sales actualizado para {fecha_venta}")
                    else:
                        # Crear nuevo registro
                        cursor.execute("""
                            INSERT INTO daily_sales (
                                show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,
                                venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                                porcentaje_ocupacion, ticketera, archivo_origen
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                        """, (
                            show_id, fecha_venta, fecha_extraccion.isoformat(), venta_diaria, monto_diario,
                            totales_show.get('vendido_total', 0), totales_show.get('recaudacion_total_ars', 0),
                            totales_show.get('disponible_total', 0), totales_show.get('porcentaje_ocupacion', 0),
                            "eticketMX", f"eticketmx_daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        ))
                        logger.info(f"✅ Daily sales creado para {fecha_venta}")
            else:
                # Si no tenemos datos de ventas diarias, crear un registro con totales actuales
                logger.info("📅 Creando registro de daily_sales con totales actuales")
                
                fecha_venta = fecha_extraccion.date()
                
                # Buscar si ya existe un registro para hoy
                cursor.execute("""
                    SELECT id FROM daily_sales 
                    WHERE show_id = %s AND fecha_venta = %s
                """, (show_id, fecha_venta))
                
                existing_record = cursor.fetchone()
                
                if existing_record:
                    # Actualizar registro existente
                    cursor.execute("""
                        UPDATE daily_sales SET
                            fecha_extraccion = %s,
                            venta_total_acumulada = %s,
                            recaudacion_total_ars = %s,
                            tickets_disponibles = %s,
                            porcentaje_ocupacion = %s,
                            updated_at = NOW()
                        WHERE show_id = %s AND fecha_venta = %s
                    """, (
                        fecha_extraccion.isoformat(),
                        totales_show.get('vendido_total', 0),
                        totales_show.get('recaudacion_total_ars', 0),
                        totales_show.get('disponible_total', 0),
                        totales_show.get('porcentaje_ocupacion', 0),
                        show_id, fecha_venta
                    ))
                    logger.info(f"📊 Daily sales actualizado para {fecha_venta}")
                else:
                    # Crear nuevo registro
                    cursor.execute("""
                        INSERT INTO daily_sales (
                            show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,
                            venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                            porcentaje_ocupacion, ticketera, archivo_origen
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        show_id, fecha_venta, fecha_extraccion.isoformat(), 0, 0,
                        totales_show.get('vendido_total', 0), totales_show.get('recaudacion_total_ars', 0),
                        totales_show.get('disponible_total', 0), totales_show.get('porcentaje_ocupacion', 0),
                        "eticketMX", f"eticketmx_daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    ))
                    logger.info(f"✅ Daily sales creado para {fecha_venta}")
                    
        except Exception as e:
            logger.error(f"❌ Error procesando daily_sales: {str(e)}")
    
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
            logger.info("=== INICIANDO SCRAPER DE E-TICKET MÉXICO ===")
            
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
            
            # Navegar directamente a Ventas Diarias
            logger.info("PASO 3: Navegando a Ventas Diarias...")
            if not self.navigate_to_ventas_diarias():
                logger.error("No se pudo navegar a Ventas Diarias")
                return False
            
            # Obtener lista de artistas
            logger.info("PASO 4: Obteniendo lista de artistas...")
            artists = self.get_all_artists()
            
            if not artists:
                logger.error("No se pudieron obtener los artistas")
                return False
            
            # Procesar cada artista y sus eventos
            logger.info(f"PASO 5: Procesando {len(artists)} artistas...")
            all_extracted_data = []
            
            for i, artist in enumerate(artists):
                logger.info(f"Procesando artista {i+1}/{len(artists)}: {artist['text']}")
                
                # Obtener eventos del artista
                events = self.select_artist_and_get_events(artist['value'], artist['text'])
                
                for j, event in enumerate(events):
                    logger.info(f"  Procesando evento {j+1}/{len(events)}: {event['text']}")
                    
                    # Extraer directamente las ventas diarias
                    daily_sales = self.get_daily_sales_for_event(
                        artist['value'],
                        artist['text'],
                        event['value'],
                        event['text']
                    )
                    
                    if daily_sales and daily_sales.get('daily_sales'):
                        # Crear estructura de datos con ventas diarias
                        event_data = {
                            'artista': artist['text'],
                            'evento_nombre': event['text'],
                            'artista_value': artist['value'],
                            'evento_codigo': event['value'],
                            'daily_sales': daily_sales,
                            'fecha_extraccion': datetime.now().isoformat()
                        }
                        all_extracted_data.append(event_data)
                        logger.info(f"  ✅ Ventas diarias extraídas para: {event['text']} ({len(daily_sales.get('daily_sales', []))} fechas)")
                    else:
                        logger.info(f"  ⏭️ Sin ventas diarias para: {event['text']}")
                    
                    # Pequeña pausa entre eventos
                    time.sleep(2)
                
                # Pequeña pausa entre artistas
                time.sleep(1)
                
                # Guardar solo en raw_data (sin procesamiento automático)
                logger.info("PASO 7: Guardando datos en raw_data...")
                success = self.save_data_to_database(all_extracted_data)
                
                if success:
                    logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
                    logger.info(f"Total de eventos procesados: {len(all_extracted_data)}")
                    logger.info(f"Eventos procesados exitosamente en base de datos")
                    return self.final_data
                else:
                    logger.error("Error procesando los datos en base de datos")
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
    logger.info("=== E-TICKET MÉXICO SCRAPER AUTOMÁTICO ===")
    logger.info("Usuario: frsierra")
    logger.info("Contraseña: ****")
    logger.info("URL de login: http://clientes.eticket.com.mx/")
    logger.info("🎭 Solo eventos con fechas actuales o futuras")
    
    # Crear y ejecutar el scraper automáticamente
    scraper = ETicketMXScraper(headless=True)
    success = scraper.run()
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Error en la ejecución del scraper")

def run_scraper_for_airflow():
    """
    Función específica para ejecutar desde Airflow
    Retorna los datos extraídos en formato JSON para enviar a base de datos
    
    Returns:
        dict: Datos completos extraídos o None si hay error
    """
    try:
        logger.info("🚀 INICIANDO SCRAPER ETICKETMX PARA AIRFLOW")
        scraper = ETicketMXScraper(headless=True)  # Headless para Airflow
        result = scraper.run()
        return result
    except Exception as e:
        logger.error(f"❌ Error ejecutando scraper para Airflow: {str(e)}")
        return None

if __name__ == "__main__":
    main()
