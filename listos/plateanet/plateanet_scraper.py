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
import random
import uuid
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from database_config import get_database_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlateanetScraper:
    def __init__(self, headless=True, test_mode=False):
        """
        Inicializa el scraper de Plateanet
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
            test_mode (bool): Si True, solo extrae datos sin guardar en BD
        """
        self.username = "producciones@daleplay.com"
        self.password = "Platea.2024"
        self.login_url = "https://www.plateanet.com/reportes/login"
        self.driver = None
        self.headless = headless
        self.test_mode = test_mode
        
        # Configuración de base de datos (solo en modo producción)
        self.db_connected = False
        if not self.test_mode:
            self.setup_database_connection()
        
        logger.info("=== INICIALIZACIÓN DEL SCRAPER PLATEANET ===")
        logger.info(f"URL de login: {self.login_url}")
        logger.info(f"Modo headless: {self.headless}")
        logger.info(f"Modo test: {self.test_mode}")
        logger.info(f"Usuario: {self.username}")

    def setup_database_connection(self):
        """Verifica conexión con la base de datos PostgreSQL"""
        try:
            logger.info("🔌 Verificando conexión con la base de datos...")
            
            # Probar conexión
            connection = get_database_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute("SELECT NOW();")
                result = cursor.fetchone()
                logger.info(f"✅ Conexión exitosa! Hora actual: {result[0]}")
                cursor.close()
                connection.close()
                self.db_connected = True
            else:
                logger.warning("⚠️ No se pudo establecer conexión con la base de datos")
                self.db_connected = False
        except Exception as e:
            logger.error(f"❌ Error verificando conexión a la base de datos: {str(e)}")
            self.db_connected = False

    def setup_driver(self):
        """Configura el driver de Chrome optimizado para contenedores"""
        try:
            logger.info("🔧 Configurando driver de Chrome para contenedor...")
            chrome_options = Options()
            
            # Configuración obligatoria para contenedores
            chrome_options.add_argument("--headless")  # Siempre headless
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Optimizar para contenedores
            
            # Anti-detección más avanzada
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # User agent más realista
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Opciones adicionales para evitar detección
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Cargar más rápido
            # chrome_options.add_argument("--disable-javascript")  # Necesario para el sitio
            
            # Intentar usar ChromeDriverManager primero
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                logger.warning(f"Error con ChromeDriverManager: {e}")
                logger.info("Intentando con driver del sistema...")
                # Fallback: usar driver del sistema
                self.driver = webdriver.Chrome(options=chrome_options)
            
            # Scripts anti-detección
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
            self.driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'es', 'en']})")
            
            # Ventana más pequeña para parecer más humano
            self.driver.set_window_size(1366, 768)
            
            logger.info("Driver configurado exitosamente con anti-detección")
            return True
            
        except Exception as e:
            logger.error(f"Error configurando driver: {str(e)}")
            return False

    def human_delay(self, min_seconds=1, max_seconds=3):
        """Pausa aleatoria para simular comportamiento humano"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def human_type(self, element, text):
        """Escribe texto de manera humana con pausas aleatorias"""
        element.clear()
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))

    def human_click(self, element):
        """Click con movimiento de mouse humano"""
        try:
            actions = ActionChains(self.driver)
            # Mover el mouse al elemento gradualmente
            actions.move_to_element(element)
            self.human_delay(0.5, 1)
            actions.click(element)
            actions.perform()
            self.human_delay(0.5, 1)
        except Exception as e:
            # Fallback a click normal
            element.click()

    def check_for_captcha(self):
        """Verifica si hay captcha presente y espera resolución manual"""
        try:
            # Buscar elementos comunes de captcha
            captcha_selectors = [
                "iframe[src*='recaptcha']",
                "iframe[src*='captcha']",
                ".captcha",
                "#captcha",
                "[class*='captcha']",
                "[id*='captcha']",
                ".g-recaptcha",
                "#g-recaptcha"
            ]
            
            captcha_found = False
            for selector in captcha_selectors:
                try:
                    captcha_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if captcha_element.is_displayed():
                        logger.warning(f"🤖 CAPTCHA detectado con selector: {selector}")
                        captcha_found = True
                        break
                except:
                    continue
            
            if captcha_found:
                logger.info("⏳ ESPERANDO RESOLUCIÓN MANUAL DEL CAPTCHA...")
                logger.info("📝 Por favor resuelve el captcha manualmente en el navegador")
                logger.info("🔄 El scraper continuará automáticamente una vez resuelto")
                
                # Esperar hasta que el captcha desaparezca o se complete el login
                max_wait = 300  # 5 minutos máximo
                wait_time = 0
                
                while wait_time < max_wait:
                    time.sleep(5)
                    wait_time += 5
                    
                    # Verificar si ya no hay captcha
                    captcha_still_present = False
                    for selector in captcha_selectors:
                        try:
                            captcha_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if captcha_element.is_displayed():
                                captcha_still_present = True
                                break
                        except:
                            continue
                    
                    # Verificar si ya estamos logueados
                    try:
                        dashboard = self.driver.find_element(By.XPATH, "//h4[contains(text(), 'Dashboard')]")
                        if dashboard:
                            logger.info("✅ CAPTCHA resuelto - Login completado")
                            return True
                    except:
                        pass
                    
                    if not captcha_still_present:
                        logger.info("✅ CAPTCHA desapareció - Continuando...")
                        return True
                    
                    logger.info(f"⏳ Esperando resolución... ({wait_time}/{max_wait}s)")
                
                logger.error("❌ Timeout esperando resolución del captcha")
                return False
            else:
                logger.info("✅ No se detectó captcha")
                return True
                
        except Exception as e:
            logger.warning(f"Error verificando captcha: {str(e)}")
            return True  # Continuar si no se puede verificar

    def login(self):
        """Realiza el login en Plateanet con comportamiento humano"""
        try:
            logger.info("=== INICIANDO LOGIN HUMANO ===")
            logger.info(f"Navegando a: {self.login_url}")
            self.driver.get(self.login_url)
            
            # Pausa humana inicial
            self.human_delay(3, 5)
            
            # Verificar captcha inmediatamente
            if not self.check_for_captcha():
                return False
            
            # Esperar a que aparezca el formulario de login
            wait = WebDriverWait(self.driver, 15)
            
            # Simular movimiento de mouse aleatorio
            actions = ActionChains(self.driver)
            actions.move_by_offset(random.randint(100, 300), random.randint(100, 300))
            actions.perform()
            
            # Log de debug para verificar la página actual
            logger.info(f"URL actual después de navegar: {self.driver.current_url}")
            logger.info(f"Título de la página: {self.driver.title}")
            
            # Buscar el campo de usuario por placeholder
            logger.info("Buscando campo de usuario...")
            user_field = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//input[@placeholder='Ingresar usuario...']")
            ))
            logger.info("Campo de usuario encontrado")
            
            # Scroll suave hacia el campo
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth'});", user_field)
            self.human_delay(1, 2)
            
            # Click humano y escritura humana
            self.human_click(user_field)
            self.human_type(user_field, self.username)
            logger.info(f"Usuario ingresado humanamente: {self.username}")
            
            self.human_delay(1, 2)
            
            # Buscar el campo de contraseña por placeholder
            password_field = self.driver.find_element(
                By.XPATH, "//input[@placeholder='Ingresar contraseña...']"
            )
            
            # Click humano y escritura humana
            self.human_click(password_field)
            self.human_type(password_field, self.password)
            logger.info("Contraseña ingresada humanamente")
            
            self.human_delay(2, 3)
            
            # Verificar captcha antes del submit
            if not self.check_for_captcha():
                return False
            
            # Buscar y hacer clic en el botón de login
            login_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Login al Dashboard')]")
            ))
            
            # Scroll hacia el botón
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth'});", login_button)
            self.human_delay(1, 2)
            
            # Click humano
            self.human_click(login_button)
            logger.info("Botón de login clickeado humanamente")
            
            # Verificar captcha después del submit
            if not self.check_for_captcha():
                return False
            
            # Esperar a que aparezca el dashboard con timeout más largo
            try:
                wait_dashboard = WebDriverWait(self.driver, 30)
                wait_dashboard.until(EC.presence_of_element_located(
                    (By.XPATH, "//h4[contains(text(), 'Dashboard')]")
                ))
                logger.info("✅ Login exitoso - Dashboard cargado")
                
                # Pausa final humana
                self.human_delay(3, 5)
                return True
                
            except Exception as dashboard_error:
                # Intentar detectar otros indicadores de login exitoso
                try:
                    # Buscar elementos del dashboard
                    dashboard_indicators = [
                        "//select[preceding-sibling::label[contains(., 'Teatro')]]",
                        "//div[contains(@class, 'dashboard')]",
                        "//h4[contains(text(), 'Dashboard')]"
                    ]
                    
                    for indicator in dashboard_indicators:
                        element = self.driver.find_element(By.XPATH, indicator)
                        if element:
                            logger.info("✅ Login exitoso - Elemento del dashboard encontrado")
                            return True
                            
                except:
                    pass
                
                logger.error(f"❌ Error esperando dashboard: {str(dashboard_error)}")
                logger.info("🔍 Verificando URL actual...")
                current_url = self.driver.current_url
                logger.info(f"URL actual: {current_url}")
                
                # Si estamos en una URL diferente al login, probablemente funcionó
                if "login" not in current_url.lower():
                    logger.info("✅ Login posiblemente exitoso - URL cambió")
                    return True
                
                return False
            
        except Exception as e:
            logger.error(f"❌ Error en login: {str(e)}")
            logger.info("🔍 Información de debug:")
            try:
                current_url = self.driver.current_url
                page_title = self.driver.title
                logger.info(f"URL actual: {current_url}")
                logger.info(f"Título de página: {page_title}")
            except:
                pass
            return False

    def get_available_theaters(self):
        """Obtiene todos los teatros disponibles"""
        try:
            logger.info("=== OBTENIENDO TEATROS DISPONIBLES ===")
            
            # Buscar el dropdown de teatros
            theater_select = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//select[preceding-sibling::label[contains(., 'Teatro')]]")
                )
            )
            
            # Obtener todas las opciones
            select_obj = Select(theater_select)
            theaters = []
            
            for option in select_obj.options:
                if option.get_attribute('value') and option.get_attribute('value').strip():
                    theaters.append({
                        'value': option.get_attribute('value'),
                        'text': option.text.strip()
                    })
            
            logger.info(f"Teatros encontrados: {len(theaters)}")
            for theater in theaters:
                logger.info(f"  - {theater['text']} (valor: {theater['value']})")
            
            return theaters
            
        except Exception as e:
            logger.error(f"Error obteniendo teatros: {str(e)}")
            return []

    def get_available_works(self, theater_value):
        """Obtiene todas las obras disponibles para un teatro"""
        try:
            logger.info(f"=== OBTENIENDO OBRAS PARA TEATRO: {theater_value} ===")
            
            # Seleccionar el teatro
            theater_select = self.driver.find_element(
                By.XPATH, "//select[preceding-sibling::label[contains(., 'Teatro')]]"
            )
            select_obj = Select(theater_select)
            select_obj.select_by_value(theater_value)
            
            # Esperar a que se actualice el dropdown de obras
            time.sleep(2)
            
            # Buscar el dropdown de obras
            work_select = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//select[preceding-sibling::label[contains(., 'Obra')]]")
                )
            )
            
            # Obtener todas las opciones
            select_obj = Select(work_select)
            works = []
            
            for option in select_obj.options:
                if option.get_attribute('value') and option.get_attribute('value').strip():
                    works.append({
                        'value': option.get_attribute('value'),
                        'text': option.text.strip()
                    })
            
            logger.info(f"Obras encontradas: {len(works)}")
            for work in works:
                logger.info(f"  - {work['text']} (valor: {work['value']})")
            
            return works
            
        except Exception as e:
            logger.error(f"Error obteniendo obras: {str(e)}")
            return []

    def get_available_functions(self, theater_value, work_value):
        """Obtiene todas las funciones disponibles para una obra"""
        try:
            logger.info(f"=== OBTENIENDO FUNCIONES PARA OBRA: {work_value} ===")
            
            # Seleccionar la obra
            work_select = self.driver.find_element(
                By.XPATH, "//select[preceding-sibling::label[contains(., 'Obra')]]"
            )
            select_obj = Select(work_select)
            select_obj.select_by_value(work_value)
            
            # Esperar a que se actualice el dropdown de funciones
            time.sleep(2)
            
            # Buscar el dropdown de funciones
            function_select = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//select[preceding-sibling::label[contains(., 'Función')]]")
                )
            )
            
            # Obtener todas las opciones
            select_obj = Select(function_select)
            functions = []
            
            for option in select_obj.options:
                if option.get_attribute('value') and option.get_attribute('value').strip():
                    functions.append({
                        'value': option.get_attribute('value'),
                        'text': option.text.strip(),
                        'date_string': option.text.strip()
                    })
            
            logger.info(f"Funciones encontradas: {len(functions)}")
            for function in functions:
                logger.info(f"  - {function['text']} (valor: {function['value']})")
            
            return functions
            
        except Exception as e:
            logger.error(f"Error obteniendo funciones: {str(e)}")
            return []

    def parse_function_date(self, date_string):
        """Parsea la fecha de la función"""
        try:
            # Buscar patrón de fecha en formato dd/mm/yyyy
            date_match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_string)
            if date_match:
                day, month, year = date_match.groups()
                function_date = datetime(int(year), int(month), int(day)).date()
                return function_date
            return None
        except Exception as e:
            logger.warning(f"No se pudo parsear la fecha: {date_string} - {str(e)}")
            return None

    def is_future_function(self, date_string):
        """Verifica si la función es futura"""
        try:
            function_date = self.parse_function_date(date_string)
            if function_date:
                today = datetime.now().date()
                return function_date >= today
            return False
        except Exception as e:
            logger.warning(f"Error verificando fecha futura: {str(e)}")
            return False

    def extract_analytics_data(self, theater_data, work_data, function_data):
        """Extrae todos los datos analíticos de la función actual"""
        try:
            logger.info(f"=== EXTRAYENDO DATOS ANALÍTICOS ===")
            logger.info(f"Teatro: {theater_data['text']}")
            logger.info(f"Obra: {work_data['text']}")
            logger.info(f"Función: {function_data['text']}")
            
            # Esperar a que carguen los datos analíticos
            time.sleep(3)
            
            extracted_data = {
                "theater": theater_data,
                "work": work_data,
                "function": function_data,
                "extraction_date": datetime.now().isoformat(),
                "analytics": {}
            }
            
            # 1. Entradas vendidas
            try:
                # Buscar el número total de entradas vendidas
                sold_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'de')]//parent::div[contains(@class, 'widget-numbers')]"
                )
                sold_text = sold_element.text.strip()
                sold_match = re.search(r'(\d+)\s+de\s+(\d+)', sold_text)
                
                if sold_match:
                    sold_tickets = int(sold_match.group(1))
                    total_capacity = int(sold_match.group(2))
                    
                    extracted_data["analytics"]["tickets_sold"] = sold_tickets
                    extracted_data["analytics"]["total_capacity"] = total_capacity
                    
                    # Buscar porcentaje vendido
                    percentage_element = self.driver.find_element(
                        By.XPATH, "//span[contains(text(), '%')]//parent::div[contains(@class, 'widget-numbers')]"
                    )
                    percentage_text = percentage_element.text.strip()
                    percentage_match = re.search(r'(\d+)', percentage_text)
                    
                    if percentage_match:
                        extracted_data["analytics"]["percentage_sold"] = int(percentage_match.group(1))
                
                logger.info(f"✅ Entradas vendidas: {sold_tickets}/{total_capacity} ({extracted_data['analytics'].get('percentage_sold', 0)}%)")
                
            except Exception as e:
                logger.warning(f"Error extrayendo entradas vendidas: {str(e)}")
            
            # 2. Promedio de tickets por operación
            try:
                avg_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Promedio tickets por OP')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                avg_text = avg_element.text.strip()
                avg_match = re.search(r'([\d,\.]+)', avg_text)
                
                if avg_match:
                    extracted_data["analytics"]["avg_tickets_per_operation"] = float(avg_match.group(1).replace(',', '.'))
                    logger.info(f"✅ Promedio tickets por OP: {extracted_data['analytics']['avg_tickets_per_operation']}")
                
            except Exception as e:
                logger.warning(f"Error extrayendo promedio de tickets: {str(e)}")
            
            # 3. Detalle de entradas emitidas
            try:
                # Entradas con descuento
                discount_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Entradas con descuento')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                discount_count = int(discount_element.text.strip())
                extracted_data["analytics"]["discount_tickets"] = discount_count
                
                # Entradas sin descuento
                no_discount_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'sin descuento')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                no_discount_count = int(no_discount_element.text.strip())
                extracted_data["analytics"]["full_price_tickets"] = no_discount_count
                
                # Entradas precio cero
                free_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'precio cero')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                free_count = int(free_element.text.strip())
                extracted_data["analytics"]["free_tickets"] = free_count
                
                logger.info(f"✅ Detalle entradas - Descuento: {discount_count}, Sin descuento: {no_discount_count}, Gratis: {free_count}")
                
            except Exception as e:
                logger.warning(f"Error extrayendo detalle de entradas: {str(e)}")
            
            # 4. Precios promedio
            try:
                # Precio full promedio
                full_price_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Precio full promedio')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                full_price_text = full_price_element.text.strip()
                full_price_match = re.search(r'\$?([\d,.]+)', full_price_text)
                
                if full_price_match:
                    full_price = full_price_match.group(1).replace(',', '').replace('.', '')
                    extracted_data["analytics"]["avg_full_price"] = int(full_price)
                
                # Precio mínimo
                min_price_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Precio mínimo')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                min_price_text = min_price_element.text.strip()
                min_price_match = re.search(r'\$?([\d,.]+)', min_price_text)
                
                if min_price_match:
                    min_price = min_price_match.group(1).replace(',', '').replace('.', '')
                    extracted_data["analytics"]["min_price"] = int(min_price)
                
                # Precio máximo
                max_price_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Precio máximo')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                max_price_text = max_price_element.text.strip()
                max_price_match = re.search(r'\$?([\d,.]+)', max_price_text)
                
                if max_price_match:
                    max_price = max_price_match.group(1).replace(',', '').replace('.', '')
                    extracted_data["analytics"]["max_price"] = int(max_price)
                
                logger.info(f"✅ Precios - Full: ${extracted_data['analytics'].get('avg_full_price', 0)}, Min: ${extracted_data['analytics'].get('min_price', 0)}, Max: ${extracted_data['analytics'].get('max_price', 0)}")
                
            except Exception as e:
                logger.warning(f"Error extrayendo precios: {str(e)}")
            
            # 5. Disponibilidad de sala
            try:
                # Butacas ocupadas
                occupied_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Butacas ocupadas')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                occupied_count = int(occupied_element.text.strip())
                extracted_data["analytics"]["occupied_seats"] = occupied_count
                
                # Butacas reservadas
                reserved_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Butacas reservadas')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                reserved_count = int(reserved_element.text.strip())
                extracted_data["analytics"]["reserved_seats"] = reserved_count
                
                # Butacas libres
                free_seats_element = self.driver.find_element(
                    By.XPATH, "//div[contains(text(), 'Butacas libres')]//following-sibling::div[contains(@class, 'widget-numbers')]"
                )
                free_seats_count = int(free_seats_element.text.strip())
                extracted_data["analytics"]["free_seats"] = free_seats_count
                
                logger.info(f"✅ Disponibilidad - Ocupadas: {occupied_count}, Reservadas: {reserved_count}, Libres: {free_seats_count}")
                
            except Exception as e:
                logger.warning(f"Error extrayendo disponibilidad: {str(e)}")
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos analíticos: {str(e)}")
            return None

    def find_show_id_automatically(self, artist, venue, function_date):
        """Busca automáticamente el show_id en la BD basándose en artista, venue y fecha"""
        try:
            conn = get_database_connection()
            if not conn:
                logger.warning("⚠️ No se pudo conectar a BD para buscar show_id")
                return None
            
            cursor = conn.cursor()
            
            logger.info(f"🔍 Buscando show para: {artist} en {venue} ({function_date})")
            
            # Convertir fecha string a datetime para comparación
            try:
                fecha_funcion = datetime.strptime(function_date, "%d/%m/%Y %H:%M")
            except:
                logger.warning(f"⚠️ Error parseando fecha: {function_date}")
                fecha_funcion = None
            
            # Búsqueda directa y simple por artista y venue
            logger.info(f"🔍 Búsqueda directa: {artist} en {venue}")
            
            search_query = """
                SELECT id, artista, venue, fecha_show, capacidad_total 
                FROM shows 
                WHERE UPPER(artista) = UPPER(%s)
                AND UPPER(venue) = UPPER(%s)
                AND ticketera = 'plateanet'
                ORDER BY fecha_show DESC
                LIMIT 1
            """
            
            cursor.execute(search_query, (artist, venue))
            results = cursor.fetchall()
            
            if results:
                show = results[0]
                show_id = show[0]
                db_artista = show[1]
                db_venue = show[2]
                db_fecha = show[3]
                db_capacidad = show[4] if len(show) > 4 else 0
                
                logger.info(f"✅ MATCH ENCONTRADO:")
                logger.info(f"   Show ID: {show_id}")
                logger.info(f"   Artista BD: {db_artista}")
                logger.info(f"   Venue BD: {db_venue}")
                logger.info(f"   Fecha BD: {db_fecha}")
                logger.info(f"   Capacidad: {db_capacidad}")
                
                cursor.close()
                conn.close()
                return show_id
            else:
                logger.warning(f"⚠️ No se encontró show_id para: {artist} en {venue}")
                cursor.close()
                conn.close()
                return None
            
        except Exception as e:
            logger.error(f"❌ Error buscando show_id: {e}")
            return None

    def upsert_daily_sales_record(self, show_id, total_actual, analytics, function_date):
        """Inserta o actualiza registro de daily_sales usando método diferencial"""
        try:
            conn = get_database_connection()
            if not conn:
                logger.error("❌ No se pudo conectar a la base de datos para UPSERT")
                return "error"
            
            cursor = conn.cursor()
            
            # Fecha de hoy para daily_sales
            fecha_venta = datetime.now().date()
            fecha_extraccion = datetime.now()
            
            logger.info(f"💾 UPSERT daily_sales para show {show_id}, fecha {fecha_venta}")
            
            # 1. Buscar último registro para calcular venta diaria
            last_query = """
                SELECT venta_total_acumulada, fecha_venta, recaudacion_total_ars FROM daily_sales 
                WHERE show_id = %s 
                ORDER BY fecha_venta DESC 
                LIMIT 1
            """
            cursor.execute(last_query, (show_id,))
            last_record = cursor.fetchone()
            
            # Calcular recaudación actual
            precio_promedio = analytics.get('avg_full_price', 35000)  # Default del scraper
            recaudacion_actual = int(total_actual * precio_promedio)
            
            # 2. Calcular venta diaria y monto diario (método diferencial)
            if last_record:
                total_anterior, fecha_anterior, recaudacion_anterior = last_record
                venta_diaria = total_actual - total_anterior
                monto_diario_ars = recaudacion_actual - recaudacion_anterior
                logger.info(f"📊 Cálculo diferencial: {total_actual} - {total_anterior} = {venta_diaria}")
                logger.info(f"💰 Cálculo monto: ${recaudacion_actual:,} - ${recaudacion_anterior:,} = ${monto_diario_ars:,}")
            else:
                venta_diaria = total_actual
                monto_diario_ars = recaudacion_actual  # Primer registro: monto diario = recaudación total
                logger.info(f"📊 Primer registro: venta_diaria = {venta_diaria}, monto_diario = ${monto_diario_ars:,}")
            
            # Evitar valores negativos
            venta_diaria = max(0, venta_diaria)
            
            # 3. Calcular otros campos
            capacidad_total = analytics.get('total_capacity', 0)
            tickets_disponibles = capacidad_total - total_actual if capacidad_total > 0 else 0
            porcentaje_ocupacion = (total_actual / capacidad_total * 100) if capacidad_total > 0 else 0.0
            recaudacion_total_ars = recaudacion_actual
            
            # 4. Verificar si ya existe registro para hoy
            check_query = """
                SELECT id, venta_diaria FROM daily_sales 
                WHERE show_id = %s AND fecha_venta = %s
            """
            cursor.execute(check_query, (show_id, fecha_venta))
            existing_record = cursor.fetchone()
            
            if existing_record:
                existing_id, existing_venta = existing_record
                
                # Solo actualizar si cambió la venta diaria
                if existing_venta != venta_diaria:
                    update_query = """
                        UPDATE daily_sales SET
                            venta_diaria = %s,
                            monto_diario_ars = %s,
                            venta_total_acumulada = %s,
                            recaudacion_total_ars = %s,
                            tickets_disponibles = %s,
                            porcentaje_ocupacion = %s,
                            fecha_extraccion = %s,
                            ticketera = %s,
                            updated_at = NOW()
                        WHERE show_id = %s AND fecha_venta = %s
                    """
                    cursor.execute(update_query, (
                        venta_diaria,
                        monto_diario_ars,
                        total_actual,
                        recaudacion_total_ars,
                        tickets_disponibles,
                        round(porcentaje_ocupacion, 2),
                        fecha_extraccion,
                        'plateanet',
                        show_id,
                        fecha_venta
                    ))
                    conn.commit()
                    logger.info(f"🔄 Registro actualizado: {venta_diaria} ventas diarias")
                    cursor.close()
                    conn.close()
                    return "updated"
                else:
                    logger.info(f"⏭️ Sin cambios: {venta_diaria} ventas diarias")
                    cursor.close()
                    conn.close()
                    return "skipped"
            else:
                # Insertar nuevo registro
                insert_query = """
                    INSERT INTO daily_sales (
                        show_id, fecha_venta, fecha_extraccion,
                        venta_diaria, monto_diario_ars,
                        venta_total_acumulada, recaudacion_total_ars,
                        tickets_disponibles, porcentaje_ocupacion,
                        ticketera, archivo_origen
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                cursor.execute(insert_query, (
                    show_id,
                    fecha_venta,
                    fecha_extraccion,
                    venta_diaria,
                    monto_diario_ars,
                    total_actual,
                    recaudacion_total_ars,
                    tickets_disponibles,
                    round(porcentaje_ocupacion, 2),
                    'plateanet',
                    'plateanet_scraper.py'
                ))
                conn.commit()
                logger.info(f"➕ Nuevo registro creado: {venta_diaria} ventas diarias")
                cursor.close()
                conn.close()
                return "inserted"
                
        except Exception as e:
            logger.error(f"❌ Error en UPSERT daily_sales: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return "error"

    def simulate_daily_sales_calculation(self, show_id, analytics):
        """Simula el cálculo diferencial de ventas diarias para modo test"""
        try:
            conn = get_database_connection()
            if not conn:
                print(f"   ❌ No se pudo conectar a BD para simulación")
                return
            
            cursor = conn.cursor()
            
            # Datos actuales de PlateaNet
            total_actual = analytics.get('tickets_sold', 0)
            precio_promedio = analytics.get('avg_full_price', 35000)
            capacidad_total = analytics.get('total_capacity', 0)
            
            print(f"   📊 DATOS ACTUALES DE PLATEANET:")
            print(f"      🎫 Total vendido: {total_actual} tickets")
            print(f"      💰 Precio promedio: ${precio_promedio:,}")
            print(f"      🏛️ Capacidad total: {capacidad_total}")
            
            # 1. Buscar último registro para calcular venta diaria
            last_query = """
                SELECT venta_total_acumulada, fecha_venta, venta_diaria, recaudacion_total_ars
                FROM daily_sales 
                WHERE show_id = %s 
                ORDER BY fecha_venta DESC 
                LIMIT 1
            """
            cursor.execute(last_query, (show_id,))
            last_record = cursor.fetchone()
            
            # 2. Calcular venta diaria y monto diario (método diferencial)
            if last_record:
                total_anterior, fecha_anterior, venta_anterior, recaudacion_anterior = last_record
                venta_diaria = total_actual - total_anterior
                recaudacion_actual = total_actual * precio_promedio
                monto_diario = recaudacion_actual - recaudacion_anterior
                
                print(f"   📈 CÁLCULO DIFERENCIAL:")
                print(f"      📅 Último registro: {fecha_anterior}")
                print(f"      🎫 Total anterior: {total_anterior} tickets")
                print(f"      🎫 Total actual: {total_actual} tickets")
                print(f"      ➖ VENTA DIARIA: {venta_diaria} tickets")
                print(f"      💰 Recaudación anterior: ${recaudacion_anterior:,}")
                print(f"      💰 Recaudación actual: ${recaudacion_actual:,}")
                print(f"      ➖ MONTO DIARIO: ${monto_diario:,}")
                
                if venta_diaria == 0:
                    print(f"      ⚠️ Sin ventas nuevas (diferencia = 0)")
                elif venta_diaria > 0:
                    print(f"      ✅ {venta_diaria} tickets vendidos desde el último registro")
                else:
                    print(f"      🔄 Diferencia negativa (posible devolución)")
            else:
                venta_diaria = total_actual
                recaudacion_actual = total_actual * precio_promedio
                monto_diario = recaudacion_actual  # Primer registro: monto diario = recaudación total
                
                print(f"   🆕 PRIMER REGISTRO:")
                print(f"      🎫 VENTA DIARIA: {venta_diaria} tickets")
                print(f"      💰 MONTO DIARIO: ${monto_diario:,}")
            
            # 3. Calcular otros campos
            tickets_disponibles = capacidad_total - total_actual if capacidad_total > 0 else 0
            porcentaje_ocupacion = (total_actual / capacidad_total * 100) if capacidad_total > 0 else 0.0
            recaudacion_total = recaudacion_actual  # Usar la recaudación ya calculada
            
            print(f"   📊 CÁLCULOS FINALES:")
            print(f"      🎫 Venta diaria: {venta_diaria}")
            print(f"      💰 Monto diario: ${monto_diario:,}")
            print(f"      📈 Venta total acumulada: {total_actual}")
            print(f"      💰 Recaudación total: ${recaudacion_total:,}")
            print(f"      🆓 Tickets disponibles: {tickets_disponibles}")
            print(f"      📊 Ocupación: {porcentaje_ocupacion:.2f}%")
            
            # 4. Simular query SQL que se ejecutaría
            print(f"   💾 QUERY SQL SIMULADA:")
            print(f"      INSERT/UPDATE daily_sales SET")
            print(f"         venta_diaria = {venta_diaria},")
            print(f"         monto_diario_ars = {monto_diario},")
            print(f"         venta_total_acumulada = {total_actual},")
            print(f"         recaudacion_total_ars = {recaudacion_total},")
            print(f"         tickets_disponibles = {tickets_disponibles},")
            print(f"         porcentaje_ocupacion = {porcentaje_ocupacion:.2f}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"   ❌ Error en simulación: {str(e)}")

    def show_test_results(self, all_data):
        """Muestra los datos extraídos en modo test sin guardar en BD"""
        try:
            print("\n" + "="*80)
            print("🧪 MODO TEST - DATOS EXTRAÍDOS DE PLATEANET")
            print("="*80)
            print(f"📅 Fecha de extracción: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            if not all_data:
                print("❌ No se extrajeron datos")
                return
            
            print(f"📊 TOTAL FUNCIONES EXTRAÍDAS: {len(all_data)}")
            print("-" * 80)
            
            # Agrupar por teatro y obra
            theaters_summary = {}
            for func_data in all_data:
                theater = func_data['theater']['text']
                work = func_data['work']['text']
                function = func_data['function']
                analytics = func_data.get('analytics', {})
                
                if theater not in theaters_summary:
                    theaters_summary[theater] = {}
                if work not in theaters_summary[theater]:
                    theaters_summary[theater][work] = []
                
                theaters_summary[theater][work].append({
                    'function': function,
                    'analytics': analytics
                })
            
            # Mostrar datos detallados
            for i, func_data in enumerate(all_data, 1):
                theater = func_data['theater']['text']
                work = func_data['work']['text']
                function = func_data['function']
                analytics = func_data.get('analytics', {})
                
                print(f"\n🎭 FUNCIÓN {i}: {work}")
                print(f"   🏛️ Teatro: {theater}")
                print(f"   📅 Fecha/Hora: {function.get('text', 'N/A')}")
                print(f"   🗓️ Fecha string: {function.get('date_string', 'N/A')}")
                print(f"   🆔 Value: {function.get('value', 'N/A')}")
                
                # Mostrar analytics si existen
                if analytics:
                    print(f"   📊 ANALYTICS:")
                    print(f"      🎫 Capacidad total: {analytics.get('total_capacity', 'N/A')}")
                    print(f"      ✅ Tickets vendidos: {analytics.get('tickets_sold', 'N/A')}")
                    print(f"      🔄 Tickets disponibles: {analytics.get('tickets_available', 'N/A')}")
                    print(f"      💰 Recaudación: ${analytics.get('total_revenue', 'N/A')}")
                    print(f"      📈 Ocupación: {analytics.get('occupation_percentage', 'N/A')}%")
                else:
                    print(f"   ⚠️ Sin datos de analytics")
                
                # Extraer artista del work
                if ' - ' in work:
                    artist = work.split(' - ')[0].strip()
                else:
                    artist = work.strip()
                
                print(f"   🎤 Artista extraído: {artist}")
                
                # Verificar si es función futura
                is_future = self.is_future_function(function.get('date_string', ''))
                print(f"   🔮 Es función futura: {'✅ Sí' if is_future else '❌ No'}")
                
                # MAPEO AUTOMÁTICO DE SHOW Y SIMULACIÓN DE CÁLCULO DIFERENCIAL
                if self.test_mode:
                    show_id = self.find_show_id_automatically(
                        artist, 
                        theater, 
                        function.get('date_string', '')
                    )
                    if show_id:
                        print(f"   🎯 Show ID encontrado: {show_id}")
                        
                        # SIMULAR CÁLCULO DIFERENCIAL DE VENTAS DIARIAS
                        self.simulate_daily_sales_calculation(show_id, analytics)
                    else:
                        print(f"   ❌ Show ID no encontrado en BD")
            
            print(f"\n✅ RESUMEN POR TEATRO:")
            for theater, works in theaters_summary.items():
                print(f"   🏛️ {theater}:")
                for work, functions in works.items():
                    print(f"      🎭 {work}: {len(functions)} funciones")
            
            print(f"\n📊 RESUMEN FINAL:")
            print(f"   🎭 Total funciones: {len(all_data)}")
            print(f"   🏛️ Total teatros: {len(theaters_summary)}")
            total_works = sum(len(works) for works in theaters_summary.values())
            print(f"   🎪 Total obras: {total_works}")
            print(f"   🧪 Modo TEST: Sin guardar en BD")
            print("="*80)
            
        except Exception as e:
            logger.error(f"❌ Error mostrando resultados de test: {e}")

    def save_to_database_DEPRECATED(self, all_data):
        """Guarda todos los datos extraídos en la base de datos"""
        try:
            if not self.db_connected:
                logger.warning("⚠️ Base de datos no conectada, no se pueden guardar datos")
                return False
            
            # Filtrar solo funciones futuras
            future_functions = []
            for data in all_data:
                if self.is_future_function(data['function']['date_string']):
                    future_functions.append(data)
            
            if not future_functions:
                logger.warning("No se encontraron funciones futuras para guardar")
                return False
            
            logger.info(f"💾 Guardando {len(future_functions)} funciones en la base de datos...")
            
            # Procesar cada función individualmente
            processed_count = 0
            for func_data in future_functions:
                if self.save_single_function_to_database(func_data):
                    processed_count += 1
            
            logger.info(f"✅ {processed_count}/{len(future_functions)} funciones guardadas exitosamente")
            
            # Resumen por teatro y obra
            theaters_summary = {}
            for func in future_functions:
                theater = func['theater']['text']
                work = func['work']['text']
                
                if theater not in theaters_summary:
                    theaters_summary[theater] = {}
                if work not in theaters_summary[theater]:
                    theaters_summary[theater][work] = 0
                theaters_summary[theater][work] += 1
            
            logger.info("📋 Resumen por teatro:")
            for theater, works in theaters_summary.items():
                logger.info(f"  🏛️ {theater}:")
                for work, count in works.items():
                    logger.info(f"    🎭 {work}: {count} funciones")
            
            return processed_count > 0
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos en base de datos: {str(e)}")
            return False

    def save_single_function_to_database(self, func_data):
        """Guarda una función individual en la base de datos"""
        try:
            # Conectar a la base de datos
            conn = get_database_connection()
            if not conn:
                logger.error("❌ No se pudo conectar a la base de datos")
                return False
            
            # Extraer información de la función
            theater = func_data['theater']['text']
            work = func_data['work']['text']
            function_date = func_data['function']['date_string']
            
            # Extraer artista del work (formato: "ARTISTA - TOUR")
            if ' - ' in work:
                artist = work.split(' - ')[0].strip()
            else:
                artist = work.strip()
            
            # Calcular totales del show
            analytics = func_data.get('analytics', {})
            totales_show = self.calculate_show_totals(analytics)
            
            # Crear JSON individual para esta función
            json_individual = {
                "artista": artist,
                "venue": theater,
                "fecha_evento": function_date,
                "url": self.login_url,
                "totales_show": totales_show,
                "function_data": func_data,
                "metadata": {
                    "usuario_utilizado": self.username,
                    "fecha_extraccion": datetime.now().isoformat(),
                    "source": "Plateanet"
                }
            }
            
            # Insertar en raw_data
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO raw_data (
                    id, ticketera, artista, venue, fecha_show, 
                    fecha_extraccion, json_data, archivo_origen, 
                    url_origen, procesado, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            # Generar UUID único
            record_id = str(uuid.uuid4())
            
            # Fecha de extracción en UTC
            fecha_extraccion_utc = datetime.utcnow()
            
            # Fecha del show (convertir de string a datetime)
            try:
                fecha_show = datetime.strptime(function_date, "%d/%m/%Y %H:%M")
            except:
                fecha_show = datetime.now() + timedelta(days=60)  # Fecha por defecto
            
            cursor.execute(insert_query, (
                record_id,
                "PlateaNet",  # ticketera
                artist,  # artista
                theater,  # venue
                fecha_show,  # fecha_show
                fecha_extraccion_utc,  # fecha_extraccion
                json.dumps(json_individual),  # json_data
                f"plateanet_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}",  # archivo_origen
                self.login_url,  # url_origen
                False,  # procesado
                datetime.utcnow(),  # created_at
                datetime.utcnow()  # updated_at
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Función '{artist} - {theater}' guardada exitosamente (ID: {record_id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando función en base de datos: {str(e)}")
            return False

    def calculate_show_totals(self, analytics):
        """Calcula los totales de un show específico basándose en los datos de analytics"""
        try:
            totales_show = {
                "capacidad_total": analytics.get('total_capacity', 0),
                "vendido_total": analytics.get('tickets_sold', 0),
                "disponible_total": 0,
                "recaudacion_total_ars": 0,
                "porcentaje_ocupacion": 0
            }
            
            # Calcular disponibles
            capacidad = totales_show["capacidad_total"]
            vendido = totales_show["vendido_total"]
            totales_show["disponible_total"] = max(0, capacidad - vendido)
            
            # Calcular recaudación (precio promedio * tickets vendidos)
            avg_price = analytics.get('avg_full_price', 0)
            totales_show["recaudacion_total_ars"] = (vendido * avg_price) * 100  # Convertir a centavos
            
            # Calcular porcentaje de ocupación
            if capacidad > 0:
                totales_show["porcentaje_ocupacion"] = round((vendido / capacidad) * 100, 2)
            
            logger.info(f"📊 Totales calculados para show:")
            logger.info(f"   📊 Capacidad: {capacidad}")
            logger.info(f"   🎫 Vendido: {vendido}")
            logger.info(f"   🆓 Disponible: {totales_show['disponible_total']}")
            logger.info(f"   💰 Recaudación: ${totales_show['recaudacion_total_ars']/100}")
            logger.info(f"   📈 Ocupación: {totales_show['porcentaje_ocupacion']}%")
            
            return totales_show
            
        except Exception as e:
            logger.error(f"❌ Error calculando totales del show: {str(e)}")
            return {
                "capacidad_total": 0,
                "vendido_total": 0,
                "disponible_total": 0,
                "recaudacion_total_ars": 0,
                "porcentaje_ocupacion": 0
            }

    def close_driver(self):
        """Cierra el driver"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver cerrado")

    def run(self):
        """Ejecuta el scraper completo"""
        try:
            logger.info("=== PLATEANET SCRAPER AUTOMÁTICO ===")
            logger.info(f"Usuario: {self.username}")
            logger.info("Contraseña: ****")
            logger.info(f"URL de login: {self.login_url}")
            logger.info("🎭 Solo funciones con fechas actuales o futuras")
            
            # PASO 1: Configurar driver
            logger.info("=== INICIANDO SCRAPER DE PLATEANET ===")
            logger.info("PASO 1: Configurando driver...")
            if not self.setup_driver():
                return False
            
            # PASO 2: Login
            logger.info("PASO 2: Realizando login...")
            if not self.login():
                return False
            
            # PASO 3: Obtener teatros
            logger.info("PASO 3: Obteniendo teatros disponibles...")
            theaters = self.get_available_theaters()
            if not theaters:
                logger.error("No se encontraron teatros")
                return False
            
            all_extracted_data = []
            
            # PASO 4: Procesar cada teatro
            for theater in theaters:
                logger.info(f"PASO 4: Procesando teatro: {theater['text']}")
                
                # Obtener obras del teatro
                works = self.get_available_works(theater['value'])
                if not works:
                    logger.warning(f"No se encontraron obras para teatro: {theater['text']}")
                    continue
                
                # Procesar cada obra
                for work in works:
                    logger.info(f"PASO 5: Procesando obra: {work['text']}")
                    
                    # Obtener funciones de la obra
                    functions = self.get_available_functions(theater['value'], work['value'])
                    if not functions:
                        logger.warning(f"No se encontraron funciones para obra: {work['text']}")
                        continue
                    
                    # Procesar cada función
                    for function in functions:
                        # Verificar si es función futura
                        if not self.is_future_function(function['date_string']):
                            logger.info(f"⏭️ Función pasada omitida: {function['text']}")
                            continue
                        
                        logger.info(f"PASO 6: Procesando función futura: {function['text']}")
                        
                        # Seleccionar la función
                        try:
                            function_select = self.driver.find_element(
                                By.XPATH, "//select[preceding-sibling::label[contains(., 'Función')]]"
                            )
                            select_obj = Select(function_select)
                            select_obj.select_by_value(function['value'])
                            
                            # Esperar a que carguen los datos
                            time.sleep(3)
                            
                            # Extraer datos analíticos
                            analytics_data = self.extract_analytics_data(theater, work, function)
                            if analytics_data:
                                all_extracted_data.append(analytics_data)
                                logger.info(f"✅ Datos extraídos para: {function['text']}")
                                
                                # UPSERT DAILY_SALES en modo producción
                                if not self.test_mode:
                                    # Extraer artista del work
                                    work_text = work['text']
                                    if ' - ' in work_text:
                                        artist = work_text.split(' - ')[0].strip()
                                    else:
                                        artist = work_text.strip()
                                    
                                    # Buscar show_id automáticamente
                                    show_id = self.find_show_id_automatically(
                                        artist,
                                        theater['text'],
                                        function['date_string']
                                    )
                                    
                                    if show_id:
                                        # Obtener total actual de tickets vendidos
                                        analytics = analytics_data.get('analytics', {})
                                        total_actual = analytics.get('tickets_sold', 0)
                                        
                                        # Ejecutar UPSERT
                                        result = self.upsert_daily_sales_record(
                                            show_id, 
                                            total_actual, 
                                            analytics, 
                                            function['date_string']
                                        )
                                        
                                        if result == "updated":
                                            logger.info(f"🔄 Daily_sales actualizado para {artist}")
                                        elif result == "inserted":
                                            logger.info(f"➕ Daily_sales insertado para {artist}")
                                        elif result == "skipped":
                                            logger.info(f"⏭️ Daily_sales sin cambios para {artist}")
                                        else:
                                            logger.warning(f"⚠️ Error en daily_sales para {artist}")
                                    else:
                                        logger.warning(f"⚠️ No se pudo mapear show para {artist} en {theater['text']}")
                            
                        except Exception as e:
                            logger.error(f"Error procesando función {function['text']}: {str(e)}")
                            continue
            
            # PASO 7: Procesar datos según el modo
            if self.test_mode:
                logger.info("PASO 7: Mostrando datos en modo test...")
                if all_extracted_data:
                    self.show_test_results(all_extracted_data)
                    logger.info(f"🧪 Test completado exitosamente!")
                    logger.info(f"📊 Total funciones procesadas: {len(all_extracted_data)}")
                    return True
                else:
                    logger.warning("No se encontraron datos para mostrar")
                    return False
            else:
                logger.info("PASO 7: Datos procesados directamente via UPSERT...")
                if all_extracted_data:
                    logger.info(f"🎉 Scraper completado exitosamente!")
                    logger.info(f"📊 Total funciones procesadas: {len(all_extracted_data)}")
                    logger.info("✅ Daily_sales actualizado directamente durante extracción")
                    return True
                else:
                    logger.warning("No se encontraron datos para procesar")
                    return False
                
        except Exception as e:
            logger.error(f"Error en ejecución principal: {str(e)}")
            return False
        finally:
            # Cerrar navegador automáticamente
            logger.info("Cerrando navegador automáticamente...")
            self.close_driver()


def main():
    """Función principal para modo producción"""
    logger.info("=== PLATEANET SCRAPER - MODO PRODUCCIÓN ===")
    logger.info("Usuario: producciones@daleplay.com")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://www.plateanet.com/reportes/login")
    
    # Crear y ejecutar el scraper
    scraper = PlateanetScraper(headless=True, test_mode=False)
    success = scraper.run()
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Error en la ejecución del scraper")

def main_test():
    """Función principal para modo test"""
    logger.info("=== PLATEANET SCRAPER - MODO TEST ===")
    logger.info("Usuario: producciones@daleplay.com")
    logger.info("Contraseña: ****")
    logger.info("URL de login: https://www.plateanet.com/reportes/login")
    
    # Crear y ejecutar el scraper en modo test
    scraper = PlateanetScraper(headless=True, test_mode=True)
    success = scraper.run()
    
    if success:
        print("✅ Test ejecutado exitosamente")
    else:
        print("❌ Error en la ejecución del test")


if __name__ == "__main__":
    # Ejecutar en modo producción para actualizar daily_sales
    main()
