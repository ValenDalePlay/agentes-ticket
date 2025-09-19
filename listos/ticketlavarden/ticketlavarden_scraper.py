from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
import os
from datetime import datetime, timezone, timedelta
import json
import re
import random
import psycopg2
from database_config import get_database_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TicketLaVardenScraper:
    def __init__(self, headless=True, test_mode=False):
        """
        Inicializa el scraper de TicketLaVarden
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
            test_mode (bool): Si True, ejecuta en modo test sin guardar en BD
        """
        self.driver = None
        self.headless = headless
        self.test_mode = test_mode
        self.base_url = "https://entradaslavarden.com/auth/login/?next=/administracion"
        self.db_connection = None
        self.final_data = []
        
        # Configurar logging
        self.setup_logging()
        
        # Conectar siempre a BD; en test se usa solo lectura (no commits)
        self.setup_database_connection()
        if self.test_mode:
            logger.info("🧪 MODO TEST - Conectado a BD (solo lectura, no se guardará nada)")
        
        # Configurar carpeta para descargas (usar /tmp para Airflow)
        self.download_folder = "/tmp"
        
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('ticketlavarden_scraper.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== INICIANDO SCRAPER DE TICKETLAVARDEN ===")
    
    def setup_database_connection(self):
        """Establece conexión con la base de datos"""
        try:
            self.db_connection = get_database_connection()
            if self.db_connection:
                self.logger.info("✅ Conexión exitosa! Hora actual: " + str(datetime.now(timezone.utc)))
            else:
                self.logger.error("❌ No se pudo establecer conexión a la base de datos")
        except Exception as e:
            self.logger.error(f"Error en setup_database_connection: {e}")
    
    def setup_driver(self):
        """Configura el driver de Chrome con evasión de bots avanzada"""
        try:
            self.logger.info("Configurando driver de Chrome...")
            
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument("--headless")
                self.logger.info("Modo headless activado")
            
            # Evasión de bots avanzada
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # User agents rotativos
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            # Configurar carpeta de descargas
            chrome_options.add_experimental_option("prefs", {
                "download.default_directory": os.path.abspath(self.download_folder),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            })
            
            # Usar ChromeDriverManager
            self.logger.info("====== WebDriver manager ======")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Configurar propiedades del webdriver para evasión
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": random.choice(user_agents)
            })
            
            # Configurar plugins y languages
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                "source": """
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['es-ES', 'es', 'en']
                    });
                """
            })
            
            self.logger.info("Driver de Chrome configurado exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error configurando ChromeDriverManager: {e}")
            self.logger.info("Intentando usar ChromeDriver del sistema...")
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                self.logger.info("Driver de Chrome configurado exitosamente")
                return True
            except Exception as e2:
                self.logger.error(f"Error al configurar el driver: {e2}")
                return False
    
    def navigate_to_login(self):
        """Navega a la página de login de TicketLaVarden"""
        try:
            self.logger.info(f"Navegando a: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Esperar a que la página cargue completamente
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            self.logger.info("Página de login cargada exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al navegar a la página de login: {str(e)}")
            return False
    
    def get_page_info(self):
        """Obtiene información básica de la página"""
        try:
            title = self.driver.title
            current_url = self.driver.current_url
            
            self.logger.info(f"Título de la página: {title}")
            self.logger.info(f"URL actual: {current_url}")
            
            return {
                'title': title,
                'url': current_url
            }
            
        except Exception as e:
            self.logger.error(f"Error al obtener información de la página: {str(e)}")
            return None
    
    def debug_page_elements(self):
        """Debug: Muestra todos los elementos de input en la página"""
        try:
            self.logger.info("=== DEBUG: Analizando elementos de la página ===")
            
            # Buscar todos los inputs
            inputs = self.driver.find_elements(By.TAG_NAME, "input")
            self.logger.info(f"Total de inputs encontrados: {len(inputs)}")
            
            for i, input_elem in enumerate(inputs):
                try:
                    input_type = input_elem.get_attribute("type")
                    input_name = input_elem.get_attribute("name")
                    input_id = input_elem.get_attribute("id")
                    input_class = input_elem.get_attribute("class")
                    
                    self.logger.info(f"Input {i+1}: type='{input_type}', name='{input_name}', id='{input_id}', class='{input_class}'")
                except:
                    continue
            
            # Buscar todos los botones
            buttons = self.driver.find_elements(By.TAG_NAME, "button")
            self.logger.info(f"Total de botones encontrados: {len(buttons)}")
            
            for i, button in enumerate(buttons):
                try:
                    button_text = button.text
                    button_type = button.get_attribute("type")
                    button_class = button.get_attribute("class")
                    
                    self.logger.info(f"Botón {i+1}: text='{button_text}', type='{button_type}', class='{button_class}'")
                except:
                    continue
            
            self.logger.info("=== FIN DEBUG ===")
            
        except Exception as e:
            self.logger.error(f"Error en debug: {str(e)}")
    
    def wait_for_login_form(self):
        """Espera a que cargue el formulario de login"""
        try:
            self.logger.info("Esperando a que cargue el formulario de login...")
            
            # Esperar más tiempo para que la página cargue completamente
            time.sleep(5)
            
            # Usar los selectores exactos que encontramos en el debug
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_username"))
            )
            self.logger.info("Campo de usuario encontrado con selector: (By.ID, 'id_username')")
            
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_password"))
            )
            self.logger.info("Campo de contraseña encontrado con selector: (By.ID, 'id_password')")
            
            self.logger.info("Formulario de login cargado exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error esperando el formulario de login: {str(e)}")
            return False
    
    def login(self, username, password):
        """Realiza el login con las credenciales proporcionadas"""
        try:
            self.logger.info("Iniciando proceso de login...")
            
            # Esperar a que el formulario esté listo
            if not self.wait_for_login_form():
                return False
            
            # Buscar y llenar el campo de username usando el selector correcto
            username_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_username"))
            )
            username_field.clear()
            username_field.send_keys(username)
            self.logger.info(f"Usuario ingresado: {username}")
            
            # Buscar y llenar el campo de contraseña
            password_field = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "id_password"))
            )
            password_field.clear()
            password_field.send_keys(password)
            self.logger.info("Contraseña ingresada")
            
            # Buscar y hacer clic en el botón "ACCEDER" usando el selector correcto
            login_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' and contains(text(), 'ACCEDER')]"))
            )
            login_button.click()
            self.logger.info("Botón ACCEDER clickeado")
            
            # Esperar un momento para que procese el login
            time.sleep(5)
            
            # Verificar si el login fue exitoso
            current_url = self.driver.current_url
            self.logger.info(f"URL después del login: {current_url}")
            
            # Verificar si hay mensajes de error
            try:
                error_messages = self.driver.find_elements(By.CLASS_NAME, "alert-danger")
                if error_messages:
                    for error in error_messages:
                        self.logger.warning(f"Mensaje de error: {error.text}")
                    return False
            except:
                pass
            
            if "login" not in current_url.lower():
                self.logger.info("Login exitoso - Redirigido a otra página")
                return True
            else:
                self.logger.warning("Login posiblemente fallido - Aún en página de login")
                return False
                
        except Exception as e:
            self.logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def close(self):
        """Cierra el driver del navegador"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Driver cerrado")
        if self.db_connection:
            self.db_connection.close()
            self.logger.info("Conexión a base de datos cerrada")
    
    def calculate_event_totals(self, borderaux_data):
        """Calcula los totales del evento desde los datos del borderaux"""
        try:
            total_capacidad = 0
            total_vendidos = 0
            total_disponibles = 0
            total_recaudacion = 0
            
            # Sumar datos de todas las categorías
            for categoria in borderaux_data.get('categorias', []):
                total_capacidad += categoria.get('cupos', 0)
                total_vendidos += categoria.get('cantidad_vendida', 0)
                total_disponibles += categoria.get('cupos_libres', 0)
                total_recaudacion += categoria.get('total', 0)
            
            # Calcular porcentaje de ocupación
            porcentaje_ocupacion = (total_vendidos / total_capacidad * 100) if total_capacidad > 0 else 0
            
            totales = {
                'capacidad_total': total_capacidad,
                'vendido_total': total_vendidos,
                'disponible_total': total_disponibles,
                'recaudacion_total_ars': total_recaudacion,
                'porcentaje_ocupacion': round(porcentaje_ocupacion, 2)
            }
            
            self.logger.info(f"📊 Totales calculados para evento:")
            self.logger.info(f"  📊 Capacidad: {total_capacidad}")
            self.logger.info(f"  🎫 Vendido: {total_vendidos}")
            self.logger.info(f"  🆓 Disponible: {total_disponibles}")
            self.logger.info(f"  💰 Recaudación: ${total_recaudacion}")
            self.logger.info(f"  📈 Ocupación: {porcentaje_ocupacion:.2f}%")
            
            return totales
            
        except Exception as e:
            self.logger.error(f"Error calculando totales del evento: {e}")
            return {
                'capacidad_total': 0,
                'vendido_total': 0,
                'disponible_total': 0,
                'recaudacion_total_ars': 0,
                'porcentaje_ocupacion': 0
            }
    
    def parse_fecha_evento(self, fecha_str):
        """Parsea la fecha del evento desde el formato de TicketLaVarden"""
        try:
            self.logger.info(f"🔍 DEBUG: Parseando fecha: '{fecha_str}'")
            
            # Formato típico: "03/10/25 21:00" o "04/09/2025"
            if "/" in fecha_str:
                # Separar fecha y hora si existe
                fecha_part = fecha_str.split(" ")[0]  # Tomar solo la parte de la fecha
                parts = fecha_part.split("/")
                if len(parts) == 3:
                    day, month, year = parts
                    # Manejar años de 2 dígitos
                    if len(year) == 2:
                        year = "20" + year
                    # Formato DD/MM/YYYY para la función de PostgreSQL
                    fecha_parsed = f"{day.zfill(2)}/{month.zfill(2)}/{year}"
                    self.logger.info(f"🔍 DEBUG: Fecha parseada: '{fecha_parsed}'")
                    return fecha_parsed
            elif "-" in fecha_str:
                # Convertir formato YYYY-MM-DD a DD/MM/YYYY
                parts = fecha_str.split("-")
                if len(parts) == 3:
                    year, month, day = parts
                    fecha_parsed = f"{day.zfill(2)}/{month.zfill(2)}/{year}"
                    self.logger.info(f"🔍 DEBUG: Fecha convertida: '{fecha_parsed}'")
                    return fecha_parsed
                return fecha_str
            
            # Si no se puede parsear, usar fecha actual
            fecha_default = datetime.now().strftime("%d/%m/%Y")
            self.logger.warning(f"⚠️ No se pudo parsear la fecha '{fecha_str}', usando fecha actual: {fecha_default}")
            return fecha_default
            
        except Exception as e:
            self.logger.error(f"Error parseando fecha: {e}")
            fecha_default = datetime.now().strftime("%d/%m/%Y")
            self.logger.warning(f"⚠️ Error parseando fecha, usando fecha actual: {fecha_default}")
            return fecha_default
    
    def show_test_results(self, borderaux_data, evento_info):
        """Muestra los resultados extraídos en modo test"""
        try:
            logger.info("")
            logger.info("=" * 80)
            logger.info("🧪 RESULTADOS DEL TEST - TICKETLAVARDEN")
            logger.info("=" * 80)
            
            # Información del evento
            logger.info("📋 INFORMACIÓN DEL EVENTO:")
            logger.info(f"   🎭 Nombre: {evento_info['nombre']}")
            logger.info(f"   📅 Fecha: {evento_info['fecha']}")
            logger.info(f"   🏛️ Recinto: {evento_info['recinto']}")
            logger.info(f"   🌍 Ciudad: {evento_info['ciudad']}")
            logger.info(f"   🔗 URL Borderaux: {evento_info.get('borderaux_url', 'N/A')}")
            
            # Calcular totales
            totales = self.calculate_event_totals(borderaux_data)
            
            logger.info("")
            logger.info("📊 TOTALES DEL EVENTO:")
            logger.info(f"   🎫 Capacidad Total: {totales['capacidad_total']:,}")
            logger.info(f"   ✅ Vendido Total: {totales['vendido_total']:,}")
            logger.info(f"   🆓 Disponible Total: {totales['disponible_total']:,}")
            logger.info(f"   💰 Recaudación Total: ${totales['recaudacion_total_ars']:,}")
            logger.info(f"   📈 Ocupación: {totales['porcentaje_ocupacion']:.2f}%")
            
            # Detalles por categoría
            logger.info("")
            logger.info("🎯 DETALLE POR CATEGORÍAS:")
            for i, categoria in enumerate(borderaux_data.get('categorias', []), 1):
                logger.info(f"   {i}. {categoria['categoria']}:")
                logger.info(f"      🎫 Cupos: {categoria['cupos']:,}")
                logger.info(f"      ✅ Vendidos: {categoria['cantidad_vendida']:,}")
                logger.info(f"      🆓 Libres: {categoria['cupos_libres']:,}")
                logger.info(f"      💰 Precio PV: ${categoria['precio_punto_venta']:,}")
                logger.info(f"      💰 Precio Online: ${categoria['precio_online']:,}")
                logger.info(f"      💰 Total Categoría: ${categoria['total']:,}")
            
            # Buscar show en BD si no es modo test
            if not self.test_mode and self.db_connection:
                self.find_show_in_database(evento_info)
            elif self.test_mode:
                logger.info("")
                logger.info("🔍 MAPEO DE SHOW (SIMULADO):")
                artista = evento_info['nombre'].split(' - ')[0] if ' - ' in evento_info['nombre'] else evento_info['nombre']
                fecha_show_str = self.parse_fecha_evento(evento_info['fecha'])
                fecha_show_date = datetime.strptime(fecha_show_str, "%d/%m/%Y").date()
                logger.info(f"   🎤 Artista extraído: {artista}")
                logger.info(f"   📅 Fecha parseada: {fecha_show_date}")
                logger.info(f"   🏛️ Venue: {evento_info['recinto']}")
                logger.info(f"   🔍 Buscaría en BD: ticketera = 'ticketlavarden'")
                logger.info(f"   🧪 En modo producción se haría UPSERT a daily_sales")
            
            logger.info("")
            logger.info("=" * 80)
            logger.info("✅ TEST COMPLETADO EXITOSAMENTE")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Error mostrando resultados del test: {e}")

    def find_show_in_database(self, evento_info):
        """Busca el show en la base de datos para verificar mapeo"""
        try:
            if not self.db_connection:
                logger.warning("⚠️ No hay conexión a BD para buscar show")
                return None
                
            artista = evento_info['nombre'].split(' - ')[0] if ' - ' in evento_info['nombre'] else evento_info['nombre']
            fecha_show_str = self.parse_fecha_evento(evento_info['fecha'])
            fecha_show_date = datetime.strptime(fecha_show_str, "%d/%m/%Y").date()
            
            cursor = self.db_connection.cursor()
            
            # Buscar show existente
            show_query = """
                SELECT id, artista, venue, fecha_show, capacidad_total 
                FROM shows 
                WHERE artista ILIKE %s 
                AND ticketera = 'ticketlavarden'
                AND DATE(fecha_show) = %s
                LIMIT 1
            """
            cursor.execute(show_query, (f'%{artista}%', fecha_show_date))
            show_result = cursor.fetchone()
            
            logger.info("")
            logger.info("🔍 MAPEO DE SHOW EN BD:")
            logger.info(f"   🎤 Artista buscado: {artista}")
            logger.info(f"   📅 Fecha buscada: {fecha_show_date}")
            
            if show_result:
                show_id, db_artista, db_venue, db_fecha, db_capacidad = show_result
                logger.info(f"   ✅ SHOW ENCONTRADO:")
                logger.info(f"      🆔 ID: {show_id}")
                logger.info(f"      🎤 Artista BD: {db_artista}")
                logger.info(f"      🏛️ Venue BD: {db_venue}")
                logger.info(f"      📅 Fecha BD: {db_fecha}")
                logger.info(f"      🎫 Capacidad BD: {db_capacidad}")
                cursor.close()
                return show_id
            else:
                logger.info(f"   ❌ Show no encontrado en BD")
                logger.info(f"   ➕ En producción se crearía nuevo show")
                cursor.close()
                return None
                
        except Exception as e:
            logger.error(f"❌ Error buscando show en BD: {e}")
            return None

    def save_single_event_to_database(self, borderaux_data, evento_info):
        """Guarda los datos directamente en daily_sales, calculando ventas diarias"""
        try:
            if not self.db_connection:
                self.logger.error("❌ No hay conexión a la base de datos")
                return False
            
            # Obtener fecha de hoy
            today = datetime.now(timezone.utc).date()
            
            # Calcular totales
            totales = self.calculate_event_totals(borderaux_data)
            
            # Parsear fecha del evento
            fecha_show_str = self.parse_fecha_evento(evento_info['fecha'])
            
            # Convertir fecha_show a formato DATE para la base de datos
            fecha_show_date = datetime.strptime(fecha_show_str, "%d/%m/%Y").date()
            
            # Obtener datos del evento
            artista = evento_info['nombre'].split(' - ')[0] if ' - ' in evento_info['nombre'] else evento_info['nombre']
            venue = evento_info['recinto']
            ciudad = evento_info['ciudad']
            
            cursor = self.db_connection.cursor()
            
            # 1. Buscar el show existente
            show_query = """
                SELECT id FROM shows 
                WHERE artista ILIKE %s 
                AND ticketera = 'ticketlavarden'
                AND DATE(fecha_show) = %s
                LIMIT 1
            """
            cursor.execute(show_query, (f'%{artista}%', fecha_show_date))
            show_result = cursor.fetchone()
            
            if show_result:
                show_id = show_result[0]
                self.logger.info(f"✅ Show encontrado: {artista} - {fecha_show_date} (ID: {show_id})")
            else:
                # Crear nuevo show
                create_show_query = """
                    INSERT INTO shows (
                        ticketera, artista, venue, ciudad, fecha_show, capacidad_total,
                        created_at, updated_at
                    ) VALUES (
                        'ticketlavarden', %s, %s, %s, %s, %s, NOW(), NOW()
                    ) RETURNING id
                """
                cursor.execute(create_show_query, (
                    artista, venue, ciudad, fecha_show_date, totales['capacidad_total']
                ))
                show_id = cursor.fetchone()[0]
                self.logger.info(f"✅ Nuevo show creado: {artista} - {fecha_show_date} (ID: {show_id})")
            
            # 2. Obtener el último registro de daily_sales para calcular diferencia (excluyendo el día actual)
            last_record_query = """
                SELECT venta_total_acumulada, recaudacion_total_ars, fecha_venta
                FROM daily_sales 
                WHERE show_id = %s 
                AND fecha_venta < %s
                ORDER BY fecha_venta DESC, fecha_extraccion DESC 
                LIMIT 1
            """
            cursor.execute(last_record_query, (show_id, today))
            last_record = cursor.fetchone()
            
            # 3. Calcular ventas diarias
            if last_record:
                last_venta_total, last_recaudacion, last_fecha = last_record
                venta_diaria = max(0, totales['vendido_total'] - last_venta_total)
                monto_diario = max(0, totales['recaudacion_total_ars'] - last_recaudacion)
                self.logger.info(f"📊 Cálculo: {totales['vendido_total']} - {last_venta_total} = {venta_diaria} ventas diarias")
                self.logger.info(f"💰 Cálculo: ${totales['recaudacion_total_ars']:,} - ${last_recaudacion:,} = ${monto_diario:,} monto diario")
            else:
                # Primera extracción para este show
                venta_diaria = totales['vendido_total']
                monto_diario = totales['recaudacion_total_ars']
                self.logger.info(f"📊 Primera extracción: {venta_diaria} ventas diarias")
                self.logger.info(f"💰 Primera extracción: ${monto_diario:,} monto diario")
            
            # 4. Verificar si ya existe un registro para hoy
            existing_query = """
                SELECT id FROM daily_sales 
                WHERE show_id = %s AND fecha_venta = %s
                ORDER BY fecha_extraccion DESC LIMIT 1
            """
            cursor.execute(existing_query, (show_id, today))
            existing_record = cursor.fetchone()
            
            # Calcular precio promedio
            precio_promedio = totales['recaudacion_total_ars'] // totales['vendido_total'] if totales['vendido_total'] > 0 else 0
            
            if existing_record:
                # Mostrar qué se actualizaría
                self.logger.info("")
                self.logger.info("🔄 REGISTRO EXISTENTE - SE ACTUALIZARÍA:")
                self.logger.info(f"   📅 Fecha: {today}")
                self.logger.info(f"   🎫 Venta Diaria: {venta_diaria} tickets")
                self.logger.info(f"   💰 Monto Diario: ${monto_diario:,}")
                self.logger.info(f"   🎫 Venta Total: {totales['vendido_total']} tickets")
                self.logger.info(f"   💰 Recaudación Total: ${totales['recaudacion_total_ars']:,}")
                self.logger.info(f"   🎫 Disponibles: {totales['disponible_total']} tickets")
                self.logger.info(f"   📈 Ocupación: {totales['porcentaje_ocupacion']:.2f}%")
                self.logger.info(f"   💰 Precio Promedio: ${precio_promedio:,}")
                self.logger.info(f"   🆔 ID del registro: {existing_record[0]}")
                
                # SQL que se ejecutaría
                update_sql = f"""
                    UPDATE daily_sales SET
                        fecha_extraccion = NOW(),
                        venta_diaria = {venta_diaria},
                        monto_diario_ars = {monto_diario},
                        venta_total_acumulada = {totales['vendido_total']},
                        recaudacion_total_ars = {totales['recaudacion_total_ars']},
                        tickets_disponibles = {totales['disponible_total']},
                        porcentaje_ocupacion = {totales['porcentaje_ocupacion']},
                        precio_promedio_ars = {precio_promedio},
                        updated_at = NOW()
                    WHERE id = '{existing_record[0]}'
                """
                self.logger.info("")
                self.logger.info("📝 SQL QUE SE EJECUTARÍA:")
                self.logger.info(update_sql)
                
            else:
                # Mostrar qué se crearía
                self.logger.info("")
                self.logger.info("➕ NUEVO REGISTRO - SE CREARÍA:")
                self.logger.info(f"   📅 Fecha: {today}")
                self.logger.info(f"   🎫 Venta Diaria: {venta_diaria} tickets")
                self.logger.info(f"   💰 Monto Diario: ${monto_diario:,}")
                self.logger.info(f"   🎫 Venta Total: {totales['vendido_total']} tickets")
                self.logger.info(f"   💰 Recaudación Total: ${totales['recaudacion_total_ars']:,}")
                self.logger.info(f"   🎫 Disponibles: {totales['disponible_total']} tickets")
                self.logger.info(f"   📈 Ocupación: {totales['porcentaje_ocupacion']:.2f}%")
                self.logger.info(f"   💰 Precio Promedio: ${precio_promedio:,}")
                self.logger.info(f"   🆔 Show ID: {show_id}")
                
                # SQL que se ejecutaría
                insert_sql = f"""
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,
                        venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                        porcentaje_ocupacion, precio_promedio_ars, ticketera, created_at, updated_at
                    ) VALUES (
                        '{show_id}', '{today}', NOW(), {venta_diaria}, {monto_diario}, 
                        {totales['vendido_total']}, {totales['recaudacion_total_ars']}, 
                        {totales['disponible_total']}, {totales['porcentaje_ocupacion']}, 
                        {precio_promedio}, 'ticketlavarden', NOW(), NOW()
                    )
                """
                self.logger.info("")
                self.logger.info("📝 SQL QUE SE EJECUTARÍA:")
                self.logger.info(insert_sql)
            
            # GUARDAR EN BASE DE DATOS
            if existing_record:
                # Actualizar registro existente
                update_query = """
                    UPDATE daily_sales SET
                        fecha_extraccion = NOW(),
                        venta_diaria = %s,
                        monto_diario_ars = %s,
                        venta_total_acumulada = %s,
                        recaudacion_total_ars = %s,
                        tickets_disponibles = %s,
                        porcentaje_ocupacion = %s,
                        precio_promedio_ars = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """
                if not self.test_mode:
                    cursor.execute(update_query, (
                        venta_diaria, monto_diario, totales['vendido_total'],
                        totales['recaudacion_total_ars'], totales['disponible_total'],
                        totales['porcentaje_ocupacion'], precio_promedio, existing_record[0]
                    ))
                    self.logger.info(f"✅ Registro actualizado: {venta_diaria} ventas diarias")
                else:
                    self.logger.info("🧪 TEST: se omite UPDATE (solo log)")
            else:
                # Insertar nuevo registro
                insert_query = """
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion, venta_diaria, monto_diario_ars,
                        venta_total_acumulada, recaudacion_total_ars, tickets_disponibles,
                        porcentaje_ocupacion, precio_promedio_ars, ticketera, created_at, updated_at
                    ) VALUES (
                        %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, 'ticketlavarden', NOW(), NOW()
                    )
                """
                if not self.test_mode:
                    cursor.execute(insert_query, (
                        show_id, today, venta_diaria, monto_diario, totales['vendido_total'],
                        totales['recaudacion_total_ars'], totales['disponible_total'],
                        totales['porcentaje_ocupacion'], precio_promedio
                    ))
                    self.logger.info(f"✅ Nuevo registro insertado: {venta_diaria} ventas diarias")
                else:
                    self.logger.info("🧪 TEST: se omite INSERT (solo log)")
            
            if not self.test_mode:
                self.db_connection.commit()
            else:
                self.logger.info("🧪 TEST: se omite COMMIT")
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error guardando evento en daily_sales: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
    
    def save_data_to_database(self, all_borderaux_data):
        """Save all extracted data to database"""
        try:
            if not all_borderaux_data:
                self.logger.warning("No hay datos para guardar")
                return

            self.logger.info(f"💾 Datos ya guardados durante la extracción individual de eventos")
            self.logger.info(f"✅ Total de eventos procesados: {len(all_borderaux_data)}")

            # Los datos ya se guardaron durante la extracción individual
            # No necesitamos guardarlos nuevamente aquí

        except Exception as e:
            self.logger.error(f"Error en save_data_to_database: {e}")
    
    def run(self):
        """Ejecuta el proceso completo del scraper"""
        try:
            self.logger.info("=== INICIANDO PROCESO COMPLETO ===")
            
            # Configurar driver
            if not self.setup_driver():
                self.logger.error("No se pudo configurar el driver")
                return False
            
            # Navegar a login
            if not self.navigate_to_login():
                self.logger.error("No se pudo navegar a la página de login")
                return False
            
            # Realizar login
            username = "MA PRODUCCIONES"
            password = "MA1234"
            
            if not self.login(username, password):
                self.logger.error("Login fallido")
                return False
            
            # Obtener eventos en curso
            eventos = self.get_eventos_en_curso()
            if not eventos:
                self.logger.warning("No se encontraron eventos en curso")
                return False
            
            self.logger.info(f"Encontrados {len(eventos)} eventos en curso")
            
            # Procesar cada evento
            all_borderaux_data = []
            for i, evento in enumerate(eventos):
                self.logger.info(f"Procesando evento {i+1}/{len(eventos)}: {evento['nombre']}")
                
                # Hacer clic en borderaux
                if self.click_borderaux(evento):
                    # Extraer información del borderaux
                    borderaux_data = self.extraer_info_borderaux(evento)
                    if borderaux_data:
                        all_borderaux_data.append(borderaux_data)
                        
                        # Procesar según el modo
                        if self.test_mode:
                            # Mostrar resultados en modo test
                            self.show_test_results(borderaux_data, evento)
                        else:
                            # Guardar en base de datos en modo producción
                            self.save_single_event_to_database(borderaux_data, evento)
                    
                    # Volver a eventos
                    self.volver_a_eventos()
                else:
                    self.logger.error(f"Error procesando evento: {evento['nombre']}")
            
            # Procesar datos finales
            if self.test_mode:
                self.logger.info("")
                self.logger.info("🧪 RESUMEN DEL TEST:")
                self.logger.info(f"   📊 Total eventos procesados: {len(all_borderaux_data)}")
                self.logger.info("   🚫 Ningún dato fue guardado en la base de datos")
                self.logger.info("   ✅ Test completado exitosamente")
            else:
                # Guardar datos finales (modo producción)
                self.save_data_to_database(all_borderaux_data)
            
            self.logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
            return True
            
        except Exception as e:
            self.logger.error(f"Error en el proceso principal: {e}")
            return False
        finally:
            self.close()
    
    
    def get_eventos_en_curso(self):
        """Obtiene la lista de eventos en curso desde la tabla"""
        try:
            self.logger.info("Obteniendo eventos en curso...")
            
            # Esperar a que la tabla cargue
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "eventos_en_curso"))
            )
            
            # Buscar todas las filas de la tabla
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            self.logger.info(f"Encontrados {len(rows)} eventos en curso")
            
            eventos = []
            for i, row in enumerate(rows):
                try:
                    # Obtener información del evento
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    evento_nombre = evento_cell.find_element(By.TAG_NAME, "a").text.strip()
                    
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
                    
                    # Buscar el botón de borderaux (ícono de silla)
                    borderaux_link = row.find_element(By.CSS_SELECTOR, "a.btn-circle.btn-outline-primary i.fa.fa-chair").find_element(By.XPATH, "./..")
                    borderaux_url = borderaux_link.get_attribute("href")
                    
                    evento_info = {
                        'indice': i + 1,
                        'nombre': evento_nombre,
                        'fecha': fecha_evento,
                        'recinto': recinto,
                        'ciudad': ciudad,
                        'borderaux_url': borderaux_url
                    }
                    
                    eventos.append(evento_info)
                    self.logger.info(f"Evento {i+1}: {evento_nombre} - {fecha_evento} - {ciudad}")
                    
                except Exception as e:
                    self.logger.error(f"Error procesando fila {i+1}: {str(e)}")
                    continue
            
            return eventos
            
        except Exception as e:
            self.logger.error(f"Error obteniendo eventos en curso: {str(e)}")
            return []
    
    def click_borderaux(self, evento_info):
        """Hace clic en el botón borderaux (ícono de silla) de un evento específico"""
        try:
            self.logger.info(f"Haciendo clic en Borderaux para: {evento_info['nombre']}")
            
            # Buscar la fila del evento por su nombre
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#eventos_en_curso tbody tr")
            
            for row in rows:
                try:
                    evento_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                    evento_nombre = evento_cell.find_element(By.TAG_NAME, "a").text.strip()
                    
                    # Limpiar el nombre del evento (igual que en get_eventos_en_curso)
                    if "\n" in evento_nombre:
                        evento_nombre = evento_nombre.split("\n")[0].strip()
                    
                    self.logger.info(f"Comparando: '{evento_nombre}' con '{evento_info['nombre']}'")
                    
                    if evento_nombre == evento_info['nombre']:
                        # Encontrar y hacer clic en el botón borderaux (ícono de silla)
                        borderaux_link = row.find_element(By.CSS_SELECTOR, "a.btn-circle.btn-outline-primary i.fa.fa-chair").find_element(By.XPATH, "./..")
                        borderaux_link.click()
                        self.logger.info(f"✅ Clic exitoso en Borderaux para: {evento_info['nombre']}")
                        
                        # Esperar a que la página cargue
                        time.sleep(3)
                        return True
                        
                except Exception as e:
                    self.logger.error(f"Error procesando fila: {str(e)}")
                    continue
            
            self.logger.error(f"No se pudo encontrar el evento: {evento_info['nombre']}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error haciendo clic en borderaux: {str(e)}")
            return False
    
    def extraer_info_borderaux(self, evento_info):
        """Extrae información de la tabla de borderaux/ventas de la página actual"""
        try:
            self.logger.info(f"Extrayendo información de borderaux para: {evento_info['nombre']}")
            
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
                self.logger.info(f"Encontradas {len(rows)} filas de categorías")
                
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
                            self.logger.info(f"✅ Categoría extraída: {categoria} - Vendida: {cantidad_vendida} - Total: {total}")
                            
                    except Exception as e:
                        self.logger.error(f"Error procesando fila de categoría: {str(e)}")
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
                        self.logger.info(f"✅ Total general extraído: {info_borderaux['resumen_total']['total_general']}")
                        
                except Exception as e:
                    self.logger.error(f"Error extrayendo totales: {str(e)}")
                    
            except Exception as e:
                self.logger.error(f"Error extrayendo datos de la tabla: {str(e)}")
            
            self.logger.info(f"✅ Información de borderaux extraída exitosamente para: {evento_info['nombre']}")
            return info_borderaux
            
        except Exception as e:
            self.logger.error(f"Error extrayendo información de borderaux: {str(e)}")
            return None
    
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
    
    def volver_a_eventos(self):
        """Vuelve a la página de eventos en curso"""
        try:
            self.logger.info("Volviendo a la página de eventos en curso...")
            
            # Intentar volver usando el botón de navegación o URL directa
            self.driver.get("https://entradaslavarden.com/administracion/")
            
            # Esperar a que cargue la página
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "eventos_en_curso"))
            )
            
            self.logger.info("✅ Vuelto exitosamente a la página de eventos")
            return True
            
        except Exception as e:
            self.logger.error(f"Error volviendo a eventos: {str(e)}")
            return False

def main():
    """Función principal para ejecutar el scraper en modo producción"""
    logger.info("=== TICKETLAVARDEN SCRAPER - MODO PRODUCCIÓN ===")
    logger.info("Usuario: MA PRODUCCIONES")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://entradaslavarden.com/auth/login/?next=/administracion")
    
    scraper = TicketLaVardenScraper(headless=True, test_mode=False)
    success = scraper.run()
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Error ejecutando el scraper")
    
    return success

def main_test():
    """Función principal para ejecutar el scraper en modo test"""
    logger.info("=== TICKETLAVARDEN SCRAPER - MODO TEST ===")
    logger.info("Usuario: MA PRODUCCIONES")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://entradaslavarden.com/auth/login/?next=/administracion")
    
    scraper = TicketLaVardenScraper(headless=True, test_mode=True)
    success = scraper.run()
    
    if success:
        print("✅ Test ejecutado exitosamente")
    else:
        print("❌ Error en la ejecución del test")
    
    return success

def run_scraper_for_airflow():
    """Función para ejecutar el scraper desde Airflow"""
    scraper = TicketLaVardenScraper(headless=True, test_mode=False)
    return scraper.run()

if __name__ == "__main__":
    # Ejecutar en modo test (conecta a BD, no guarda)
    main_test()
