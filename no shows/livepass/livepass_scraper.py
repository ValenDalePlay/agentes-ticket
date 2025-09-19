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
from datetime import datetime, date, timezone, timedelta
import json
import re
import random
import psycopg2
from database_config import get_database_connection

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LivepassScraper:
    def __init__(self, headless=True):
        """
        Inicializa el scraper de Livepass
        
        Args:
            headless (bool): Si True, ejecuta el navegador en modo headless
        """
        self.driver = None
        self.headless = headless
        self.base_url = "https://livepass.com.ar/manage/sign_in"
        self.db_connection = None
        self.final_data = []
        
        # Configurar logging
        self.setup_logging()
        
        # Configurar conexión a base de datos
        self.setup_database_connection()
        
        # Configurar carpeta para descargas (usar /tmp para Airflow)
        self.download_folder = "/tmp"
        
    def setup_logging(self):
        """Configura el sistema de logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('livepass_scraper.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== INICIANDO SCRAPER DE LIVEPASS ===")
    
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
        """Navega a la página de login de Livepass"""
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
            self.logger.error(f"Error navegando a la página de login: {str(e)}")
            return False
    
    def login(self, email, password):
        """
        Realiza el login en Livepass
        
        Args:
            email (str): Email del usuario
            password (str): Contraseña del usuario
            
        Returns:
            bool: True si el login fue exitoso, False en caso contrario
        """
        try:
            self.logger.info("Iniciando proceso de login...")
            
            # Buscar campo de email usando el ID correcto
            email_field = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "manager_email"))
            )
            email_field.clear()
            email_field.send_keys(email)
            self.logger.info("Email ingresado")
            
            # Buscar campo de contraseña usando el ID correcto
            password_field = self.driver.find_element(By.ID, "manager_password")
            password_field.clear()
            password_field.send_keys(password)
            self.logger.info("Contraseña ingresada")
            
            # Buscar botón de login usando el valor correcto
            login_button = self.driver.find_element(By.XPATH, "//input[@type='submit' and @value='Ingresar']")
            login_button.click()
            self.logger.info("Botón de login clickeado")
            
            # Esperar a que se complete el login
            time.sleep(3)
            
            # Verificar si el login fue exitoso
            if "dashboard" in self.driver.current_url or "manage" in self.driver.current_url:
                self.logger.info("Login exitoso")
                return True
            else:
                self.logger.warning("Login posiblemente fallido")
                return False
                
        except Exception as e:
            self.logger.error(f"Error durante el login: {str(e)}")
            return False
    
    def get_events_list(self):
        """
        Obtiene la lista de eventos desde la tabla
        
        Returns:
            list: Lista de eventos con sus datos
        """
        try:
            self.logger.info("Obteniendo lista de eventos...")
            
            # Esperar a que la tabla de eventos cargue
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "table_events"))
            )
            
            # Buscar todos los enlaces de eventos (excluyendo botones de copiar)
            event_links = self.driver.find_elements(By.CSS_SELECTOR, "#table_events a[href*='/events/']:not([href*='/copy'])")
            
            events = []
            for i, link in enumerate(event_links):
                try:
                    # Obtener el título del evento
                    title_element = link.find_element(By.CSS_SELECTOR, "h2")
                    title = title_element.text.strip()
                    
                    # Obtener la fecha del evento
                    date_element = link.find_element(By.CSS_SELECTOR, "p")
                    date = date_element.text.strip()
                    
                    # Obtener el estado del evento
                    status_element = link.find_element(By.CSS_SELECTOR, ".event-status-chip")
                    status = status_element.text.strip()
                    
                    # Obtener la URL del evento
                    event_url = link.get_attribute("href")
                    
                    event_data = {
                        "index": i + 1,
                        "title": title,
                        "date": date,
                        "status": status,
                        "url": event_url
                    }
                    
                    events.append(event_data)
                    self.logger.info(f"Evento {i+1}: {title} - {date}")
                    
                except Exception as e:
                    self.logger.warning(f"Error procesando evento {i+1}: {str(e)}")
                    continue
            
            self.logger.info(f"Total de eventos encontrados: {len(events)}")
            return events
            
        except Exception as e:
            self.logger.error(f"Error obteniendo lista de eventos: {str(e)}")
            return []
    
    def extract_event_dashboard_data(self):
        """
        Extrae todos los datos del dashboard del evento
        
        Returns:
            dict: Diccionario con todos los datos del evento
        """
        try:
            self.logger.info("Extrayendo datos del dashboard del evento...")
            
            # Esperar más tiempo para que los elementos del dashboard carguen completamente
            time.sleep(30)
            
            # Intentar múltiples estrategias para encontrar los elementos
            event_data = {}
            
            # Resumen General - Intentar diferentes selectores
            total_sales = self._get_element_text([
                "event-dashboard-total-sales-livepass",
                "event-dashboard-total-sales",
                "[id*='total-sales']",
                "[id*='sales']"
            ])
            event_data["total_sales"] = total_sales
            self.logger.info(f"Total Ventas: {total_sales}")
            
            total_tickets = self._get_element_text([
                "event-dashboard-total-tickets-count-livepass",
                "event-dashboard-total-tickets-count",
                "[id*='total-tickets']",
                "[id*='tickets']"
            ])
            event_data["total_tickets_sold"] = total_tickets
            self.logger.info(f"Total Tickets Vendidos: {total_tickets}")
            
            occupation_percentage = self._get_element_text([
                "event-dashboard-occupation-percentage",
                "[id*='occupation']",
                "[id*='percentage']"
            ])
            event_data["occupation_percentage"] = occupation_percentage
            self.logger.info(f"% de Ocupación: {occupation_percentage}")
            
            availability = self._get_element_text([
                "event-dashboard-availability",
                "[id*='availability']"
            ])
            event_data["availability"] = availability
            self.logger.info(f"Disponibilidad: {availability}")
            
            # Resumen Diario
            today_sales = self._get_element_text([
                "event-dashboard-today-total-sales",
                "[id*='today-sales']"
            ])
            event_data["today_sales"] = today_sales
            self.logger.info(f"Total Ventas Hoy: {today_sales}")
            
            today_tickets = self._get_element_text([
                "event-dashboard-today-total-tickets-count",
                "[id*='today-tickets']"
            ])
            event_data["today_tickets_sold"] = today_tickets
            self.logger.info(f"Total Tickets Vendidos Hoy: {today_tickets}")
            
            seven_days_sales = self._get_element_text([
                "event-dashboard-last-seven-days-total-sales",
                "[id*='seven-days']"
            ])
            event_data["seven_days_sales"] = seven_days_sales
            self.logger.info(f"Promedio Ventas 7 días: {seven_days_sales}")
            
            seven_days_tickets = self._get_element_text([
                "event-dashboard-last-seven-days-total-tickets-count",
                "[id*='seven-days-tickets']"
            ])
            event_data["seven_days_tickets_sold"] = seven_days_tickets
            self.logger.info(f"Tickets Vendidos últimos 7 días: {seven_days_tickets}")
            
            # Otra Información
            total_capacity = self._get_element_text([
                "event-dashboard-total-capacity",
                "[id*='capacity']"
            ])
            event_data["total_capacity"] = total_capacity
            self.logger.info(f"Capacidad Total: {total_capacity}")
            
            kills = self._get_element_text([
                "event-dashboard-kills",
                "[id*='kills']"
            ])
            event_data["kills"] = kills
            self.logger.info(f"Kills: {kills}")
            
            hold = self._get_element_text([
                "event-dashboard-hold",
                "[id*='hold']"
            ])
            event_data["hold"] = hold
            self.logger.info(f"Hold: {hold}")
            
            comps = self._get_element_text([
                "event-dashboard-comps",
                "[id*='comps']"
            ])
            event_data["comps"] = comps
            self.logger.info(f"Comps: {comps}")
            
            # Extraer datos de la tabla de localidades
            locations_data = self.extract_locations_table_data()
            if locations_data:
                event_data["locations_breakdown"] = locations_data
                self.logger.info("Datos de tabla de localidades extraídos exitosamente")
            
            # Agregar información adicional
            event_data["scraped_at"] = datetime.now().isoformat()
            event_data["event_url"] = self.driver.current_url
            
            self.logger.info("Datos del dashboard extraídos exitosamente")
            return event_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos del dashboard: {str(e)}")
            return {}
    
    def extract_locations_table_data(self):
        """
        Extrae los datos de la tabla de localidades (asientos activos por zona)
        
        Returns:
            dict: Diccionario con los datos de localidades organizados por zona
        """
        try:
            self.logger.info("Extrayendo datos de tabla de localidades...")
            
            # Buscar la tabla de localidades
            table_selector = "#tickets-capacities-with-map .map-table"
            
            try:
                table = self.driver.find_element(By.CSS_SELECTOR, table_selector)
            except:
                self.logger.warning("No se encontró la tabla de localidades")
                return {}
            
            locations_data = {
                "zones": {},
                "total_summary": {}
            }
            
            # Extraer filas principales (zonas)
            zone_rows = table.find_elements(By.CSS_SELECTOR, "tr[data-location-target]")
            
            for row in zone_rows:
                try:
                    zone_name = row.get_attribute("data-location-target")
                    if not zone_name:
                        continue
                    
                    # Extraer datos de la zona principal
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 10:
                        zone_display_name = cells[0].text.strip()
                        
                        zone_data = {
                            "zone_name": zone_display_name,
                            "capacity": self._extract_number_from_text(cells[1].text),
                            "kills": self._extract_number_from_text(cells[2].text),
                            "holds": self._extract_number_from_text(cells[3].text),
                            "sales": self._extract_number_from_text(cells[4].text),
                            "comps": self._extract_number_from_text(cells[5].text),
                            "in_process": self._extract_number_from_text(cells[6].text),
                            "available": self._extract_number_from_text(cells[7].text),
                            "validated": self._extract_number_from_text(cells[8].text),
                            "amount": self._extract_money_from_text(cells[9].text),
                            "subsections": {}
                        }
                        
                        locations_data["zones"][zone_name] = zone_data
                        self.logger.info(f"Zona extraída: {zone_display_name}")
                        
                except Exception as e:
                    self.logger.warning(f"Error procesando zona: {str(e)}")
                    continue
            
            # Extraer totales finales
            try:
                final_row = table.find_element(By.CSS_SELECTOR, "#final-results")
                cells = final_row.find_elements(By.TAG_NAME, "td")
                
                if len(cells) >= 10:
                    locations_data["total_summary"] = {
                        "total_capacity": self._extract_number_from_text(cells[1].text),
                        "total_kills": self._extract_number_from_text(cells[2].text),
                        "total_holds": self._extract_number_from_text(cells[3].text),
                        "total_sales": self._extract_number_from_text(cells[4].text),
                        "total_comps": self._extract_number_from_text(cells[5].text),
                        "total_in_process": self._extract_number_from_text(cells[6].text),
                        "total_available": self._extract_number_from_text(cells[7].text),
                        "total_validated": self._extract_number_from_text(cells[8].text),
                        "total_amount": self._extract_money_from_text(cells[9].text)
                    }
                    self.logger.info("Totales finales extraídos")
                    
            except Exception as e:
                self.logger.warning(f"Error extrayendo totales finales: {str(e)}")
            
            self.logger.info(f"Tabla de localidades extraída: {len(locations_data['zones'])} zonas")
            return locations_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo tabla de localidades: {str(e)}")
            return {}
    
    def _extract_number_from_text(self, text):
        """Extrae un número del texto, removiendo formato"""
        try:
            # Remover espacios, puntos y otros caracteres de formato
            clean_text = re.sub(r'[^\d]', '', text.strip())
            return int(clean_text) if clean_text else 0
        except:
            return 0
    
    def _extract_money_from_text(self, text):
        """Extrae valor monetario del texto manteniendo el formato original"""
        try:
            # Mantener el formato original pero extraer también el valor numérico
            original = text.strip()
            # Extraer solo números para cálculos
            clean_text = re.sub(r'[^\d]', '', original)
            numeric_value = int(clean_text) if clean_text else 0
            
            return {
                "formatted": original,
                "numeric": numeric_value
            }
        except:
            return {
                "formatted": text.strip(),
                "numeric": 0
            }
    
    def _parse_event_date(self, date_string):
        """
        Parsea la fecha del evento y verifica si es del día actual o posterior
        
        Args:
            date_string (str): Fecha del evento en formato string
            
        Returns:
            tuple: (datetime_object, is_future_or_today)
        """
        try:
            # Limpiar la fecha de caracteres extra
            clean_date = date_string.strip()
            self.logger.info(f"Parseando fecha: {clean_date}")
            
            # Mapeo de meses abreviados en español a inglés
            spanish_months_abbr = {
                'ene': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'abr': 'Apr',
                'may': 'May', 'jun': 'Jun', 'jul': 'Jul', 'ago': 'Aug',
                'sep': 'Sep', 'oct': 'Oct', 'nov': 'Nov', 'dic': 'Dec'
            }
            
            # Mapeo de meses completos en español a inglés
            spanish_months_full = {
                'enero': 'January', 'febrero': 'February', 'marzo': 'March',
                'abril': 'April', 'mayo': 'May', 'junio': 'June',
                'julio': 'July', 'agosto': 'August', 'septiembre': 'September',
                'octubre': 'October', 'noviembre': 'November', 'diciembre': 'December'
            }
            
            # Mapeo de días de la semana en español a inglés
            spanish_days = {
                'lun': 'Mon', 'mar': 'Tue', 'mié': 'Wed', 'jue': 'Thu',
                'vie': 'Fri', 'sáb': 'Sat', 'dom': 'Sun'
            }
            
            event_date = None
            
            # Formato específico de Livepass: "Dom, 17 de Ago de 2025 21:00:00 -0300"
            if re.match(r'\w+,\s+\d+\s+de\s+\w+\s+de\s+\d+', clean_date):
                try:
                    # Extraer las partes de la fecha usando regex
                    pattern = r'(\w+),\s+(\d+)\s+de\s+(\w+)\s+de\s+(\d+)\s+(\d+):(\d+):(\d+)'
                    match = re.search(pattern, clean_date)
                    
                    if match:
                        day_name, day, month_abbr, year, hour, minute, second = match.groups()
                        
                        # Convertir mes abreviado español a número
                        month_number = None
                        month_abbr_lower = month_abbr.lower()
                        
                        month_mapping = {
                            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4,
                            'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
                            'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
                        }
                        
                        month_number = month_mapping.get(month_abbr_lower)
                        
                        if month_number:
                            event_date = datetime(
                                int(year), month_number, int(day),
                                int(hour), int(minute), int(second)
                            )
                            self.logger.info(f"Fecha parseada exitosamente usando formato Livepass: {event_date}")
                        
                except Exception as e:
                    self.logger.warning(f"Error parseando formato Livepass: {str(e)}")
            
            # Si no se pudo parsear con el formato Livepass, intentar otros formatos
            if not event_date:
                # Posibles formatos alternativos
                date_formats = [
                    "%d/%m/%Y",  # 25/12/2024
                    "%d-%m-%Y",  # 25-12-2024
                    "%Y-%m-%d",  # 2024-12-25
                    "%d de %B de %Y",  # 25 de diciembre de 2024
                    "%d %B %Y",  # 25 diciembre 2024
                    "%d/%m/%Y %H:%M",  # 25/12/2024 20:00
                    "%d-%m-%Y %H:%M",  # 25-12-2024 20:00
                ]
                
                # Preparar fecha para parseo alternativo
                date_to_parse = clean_date.lower()
                
                # Reemplazar meses en español por inglés
                for esp, eng in spanish_months_full.items():
                    date_to_parse = date_to_parse.replace(esp, eng.lower())
                
                for esp, eng in spanish_months_abbr.items():
                    date_to_parse = date_to_parse.replace(esp, eng.lower())
                
                # Intentar parsear con diferentes formatos
                for fmt in date_formats:
                    try:
                        event_date = datetime.strptime(date_to_parse, fmt)
                        self.logger.info(f"Fecha parseada con formato {fmt}: {event_date}")
                        break
                    except ValueError:
                        continue
                
                # Como último recurso, usar regex para extraer fecha básica
                if not event_date:
                    date_pattern = r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})'
                    match = re.search(date_pattern, clean_date)
                    if match:
                        day, month, year = match.groups()
                        event_date = datetime(int(year), int(month), int(day))
                        self.logger.info(f"Fecha parseada con regex: {event_date}")
            
            if event_date:
                # Comparar solo las fechas (sin hora)
                today = date.today()
                event_date_only = event_date.date()
                is_future_or_today = event_date_only >= today
                
                self.logger.info(f"Fecha parseada: {event_date_only}, Es actual o futura: {is_future_or_today}")
                return event_date, is_future_or_today
            else:
                self.logger.warning(f"No se pudo parsear la fecha: {date_string}")
                return None, False
                
        except Exception as e:
            self.logger.error(f"Error parseando fecha '{date_string}': {str(e)}")
            return None, False

    def _get_element_text(self, selectors):
        """
        Intenta obtener el texto de un elemento usando múltiples selectores
        
        Args:
            selectors (list): Lista de selectores CSS a intentar
            
        Returns:
            str: Texto del elemento o "No disponible" si no se encuentra
        """
        for selector in selectors:
            try:
                if selector.startswith('['):
                    # Selector CSS
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                else:
                    # ID
                    element = self.driver.find_element(By.ID, selector)
                
                text = element.text.strip()
                if text:  # Solo retornar si hay texto
                    return text
            except:
                continue
        
        return "No disponible"
    
    def save_event_data_to_json(self, event_data, event_title):
        """
        Guarda los datos del evento en un archivo JSON
        
        Args:
            event_data (dict): Datos del evento
            event_title (str): Título del evento para el nombre del archivo
        """
        try:
            # Crear nombre de archivo seguro
            safe_title = re.sub(r'[^\w\s-]', '', event_title).strip()
            safe_title = re.sub(r'[-\s]+', '-', safe_title)
            filename = f"{safe_title}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = os.path.join(self.download_folder, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(event_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Datos guardados en: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error guardando datos en JSON: {str(e)}")
            return None
    
    def click_event_by_index(self, event_index):
        """
        Hace clic en un evento específico por su índice
        
        Args:
            event_index (int): Índice del evento (1-based)
            
        Returns:
            bool: True si se pudo hacer clic, False en caso contrario
        """
        try:
            self.logger.info(f"Haciendo clic en el evento {event_index}...")
            
            # Buscar todos los enlaces de eventos (excluyendo botones de copiar)
            event_links = self.driver.find_elements(By.CSS_SELECTOR, "#table_events a[href*='/events/']:not([href*='/copy'])")
            
            if event_index <= 0 or event_index > len(event_links):
                self.logger.error(f"Índice de evento inválido: {event_index}")
                return False
            
            # Hacer clic en el evento específico
            event_links[event_index - 1].click()
            self.logger.info(f"Haciendo clic en evento {event_index}")
            
            # Esperar a que la página del evento cargue
            time.sleep(3)
            
            self.logger.info("Navegación al evento exitosa")
            return True
            
        except Exception as e:
            self.logger.error(f"Error haciendo clic en el evento {event_index}: {str(e)}")
            return False
    
    def process_all_events(self):
        """
        Procesa todos los eventos: extrae datos y los guarda en JSON
        
        Returns:
            list: Lista de archivos JSON creados
        """
        try:
            self.logger.info("Iniciando procesamiento de todos los eventos...")
            
            # Obtener lista de eventos al inicio
            events = self.get_events_list()
            
            if not events:
                self.logger.error("No se encontraron eventos")
                return []
            
            self.logger.info(f"Se encontraron {len(events)} eventos para procesar")
            created_files = []
            
            # Procesar cada evento usando su URL directa
            for i, event in enumerate(events, 1):
                try:
                    self.logger.info(f"Procesando evento {i}/{len(events)}: {event['title']}")
                    
                    # Validar fecha del evento antes de procesar
                    event_datetime, is_future_or_today = self._parse_event_date(event['date'])
                    
                    if not is_future_or_today:
                        self.logger.info(f"Saltando evento {i} - fecha pasada: {event['date']}")
                        continue
                    
                    self.logger.info(f"Evento {i} tiene fecha válida (actual o futura): {event['date']}")
                    
                    # Navegar directamente a la URL del evento
                    self.logger.info(f"Navegando a: {event['url']}")
                    self.driver.get(event['url'])
                    
                    # Esperar a que la página cargue
                    time.sleep(5)
                    
                    # Verificar que estamos en la página correcta
                    if "events" not in self.driver.current_url:
                        self.logger.error(f"No se pudo navegar al evento {i}")
                        continue
                    
                    # Extraer datos del dashboard
                    event_data = self.extract_event_dashboard_data()
                    
                    if event_data:
                        # Agregar información del evento
                        event_data["event_title"] = event['title']
                        event_data["event_date"] = event['date']
                        event_data["event_status"] = event['status']
                        event_data["event_url"] = event['url']
                        event_data["parsed_date"] = event_datetime.isoformat() if event_datetime else None
                        
                        # Guardar en JSON
                        filepath = self.save_event_data_to_json(event_data, event['title'])
                        
                        if filepath:
                            created_files.append(filepath)
                            self.logger.info(f"Evento {i} procesado exitosamente")
                        else:
                            self.logger.error(f"Error guardando datos del evento {i}")
                    else:
                        self.logger.error(f"No se pudieron extraer datos del evento {i}")
                    
                except Exception as e:
                    self.logger.error(f"Error procesando evento {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Procesamiento completado. {len(created_files)} archivos creados")
            return created_files
            
        except Exception as e:
            self.logger.error(f"Error en el procesamiento de eventos: {str(e)}")
            return []
    
    def click_first_event(self):
        """
        Hace clic en el primer evento de la lista
        
        Returns:
            bool: True si se pudo hacer clic, False en caso contrario
        """
        try:
            self.logger.info("Haciendo clic en el primer evento...")
            
            # Obtener la lista de eventos
            events = self.get_events_list()
            
            if not events:
                self.logger.error("No se encontraron eventos")
                return False
            
            # Buscar el primer enlace de evento (excluyendo botones de copiar)
            first_event_link = self.driver.find_element(By.CSS_SELECTOR, "#table_events a[href*='/events/']:not([href*='/copy'])")
            
            # Hacer clic en el primer evento
            first_event_link.click()
            self.logger.info(f"Haciendo clic en: {events[0]['title']}")
            
            # Esperar a que la página del evento cargue
            time.sleep(3)
            
            self.logger.info("Navegación al evento exitosa")
            return True
            
        except Exception as e:
            self.logger.error(f"Error haciendo clic en el primer evento: {str(e)}")
            return False
    
    def close_driver(self):
        """Cierra el driver del navegador"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Driver cerrado")
        if self.db_connection:
            self.db_connection.close()
            self.logger.info("Conexión a base de datos cerrada")
    
    def calculate_event_totals(self, event_data):
        """Calcula los totales del evento desde los datos extraídos"""
        try:
            # Extraer totales del resumen general
            total_sales = self._extract_number_from_text(event_data.get('total_sales', '0'))
            total_tickets_sold = self._extract_number_from_text(event_data.get('total_tickets_sold', '0'))
            total_capacity = self._extract_number_from_text(event_data.get('total_capacity', '0'))
            availability = self._extract_number_from_text(event_data.get('availability', '0'))
            occupation_percentage = self._extract_percentage_from_text(event_data.get('occupation_percentage', '0%'))
            
            # Calcular totales desde la tabla de localidades si está disponible
            if 'locations_breakdown' in event_data and 'total_summary' in event_data['locations_breakdown']:
                total_summary = event_data['locations_breakdown']['total_summary']
                total_capacity = total_summary.get('total_capacity', total_capacity)
                total_tickets_sold = total_summary.get('total_sales', total_tickets_sold)
                total_sales = total_summary.get('total_amount', {}).get('numeric', total_sales)
                availability = total_summary.get('total_available', availability)
            
            # Calcular porcentaje de ocupación si no está disponible
            if occupation_percentage == 0 and total_capacity > 0:
                occupation_percentage = (total_tickets_sold / total_capacity) * 100
            
            totales = {
                'capacidad_total': total_capacity,
                'vendido_total': total_tickets_sold,
                'disponible_total': availability,
                'recaudacion_total_ars': total_sales,
                'porcentaje_ocupacion': round(occupation_percentage, 2)
            }
            
            self.logger.info(f"📊 Totales calculados para evento:")
            self.logger.info(f"  📊 Capacidad: {total_capacity}")
            self.logger.info(f"  🎫 Vendido: {total_tickets_sold}")
            self.logger.info(f"  🆓 Disponible: {availability}")
            self.logger.info(f"  💰 Recaudación: ${total_sales}")
            self.logger.info(f"  📈 Ocupación: {occupation_percentage:.2f}%")
            
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
    
    def _extract_percentage_from_text(self, text):
        """Extrae porcentaje del texto"""
        try:
            if isinstance(text, str):
                # Buscar patrón de porcentaje
                match = re.search(r'(\d+(?:\.\d+)?)%', text)
                if match:
                    return float(match.group(1))
            return 0
        except:
            return 0
    
    def parse_fecha_evento(self, fecha_str):
        """Parsea la fecha del evento desde el formato de LivePass"""
        try:
            self.logger.info(f"🔍 DEBUG: Parseando fecha: '{fecha_str}'")
            
            # Formato típico de LivePass: "Dom, 17 de Ago de 2025 21:00:00 -0300"
            if re.match(r'\\w+,\\s+\\d+\\s+de\\s+\\w+\\s+de\\s+\\d+', fecha_str):
                try:
                    # Extraer las partes de la fecha usando regex
                    pattern = r'(\\w+),\\s+(\\d+)\\s+de\\s+(\\w+)\\s+de\\s+(\\d+)\\s+(\\d+):(\\d+):(\\d+)'
                    match = re.search(pattern, fecha_str)
                    
                    if match:
                        day_name, day, month_abbr, year, hour, minute, second = match.groups()
                        
                        # Convertir mes abreviado español a número
                        month_mapping = {
                            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4,
                            'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
                            'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
                        }
                        
                        month_number = month_mapping.get(month_abbr.lower())
                        
                        if month_number:
                            fecha_parsed = f"{year}-{month_number:02d}-{int(day):02d}"
                            self.logger.info(f"🔍 DEBUG: Fecha parseada: '{fecha_parsed}'")
                            return fecha_parsed
                        
                except Exception as e:
                    self.logger.warning(f"Error parseando formato LivePass: {e}")
            
            # Si no se puede parsear, usar fecha actual
            fecha_default = datetime.now().strftime("%Y-%m-%d")
            self.logger.warning(f"⚠️ No se pudo parsear la fecha '{fecha_str}', usando fecha actual: {fecha_default}")
            return fecha_default
            
        except Exception as e:
            self.logger.error(f"Error parseando fecha: {e}")
            fecha_default = datetime.now().strftime("%Y-%m-%d")
            self.logger.warning(f"⚠️ Error parseando fecha, usando fecha actual: {fecha_default}")
            return fecha_default
    
    def save_single_event_to_database(self, event_data, event_info):
        """Guarda los datos de un evento individual en la base de datos"""
        try:
            if not self.db_connection:
                self.logger.error("No hay conexión a la base de datos")
                return False
            
            # Calcular totales
            totales = self.calculate_event_totals(event_data)
            
            # Parsear fecha del evento
            fecha_show = self.parse_fecha_evento(event_info['date'])
            
            # Extraer artista y venue del título del evento
            evento_nombre = event_info['title']
            # Intentar extraer artista y venue del título
            if ' - ' in evento_nombre:
                artista, venue = evento_nombre.split(' - ', 1)
            else:
                artista = evento_nombre
                venue = "LivePass Venue"
            
            # Estructurar datos para la base de datos
            json_individual = {
                'ticketera': 'livepass',
                'artista': artista.strip(),
                'venue': venue.strip(),
                'fecha_show': fecha_show,
                'evento_nombre': evento_nombre,
                'ciudad': venue.strip(),  # Usar venue como ciudad por defecto
                'dashboard_data': event_data,
                'capacidad_total': totales['capacidad_total'],
                'vendido_total': totales['vendido_total'],
                'disponible_total': totales['disponible_total'],
                'recaudacion_total_ars': totales['recaudacion_total_ars'],
                'porcentaje_ocupacion': totales['porcentaje_ocupacion'],
                'fecha_extraccion': datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Insertar en la base de datos
            cursor = self.db_connection.cursor()
            
            insert_query = """
                INSERT INTO raw_data (
                    ticketera, artista, venue, fecha_show, json_data, 
                    fecha_extraccion, archivo_origen, url_origen, procesado
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
            """
            
            # Convertir fecha_extraccion a Argentina timezone (UTC-3)
            fecha_extraccion_argentina = datetime.now(timezone.utc) - timedelta(hours=3)
            
            cursor.execute(insert_query, (
                'livepass',
                json_individual['artista'],
                json_individual['venue'],
                fecha_show,
                json.dumps(json_individual, ensure_ascii=False),
                fecha_extraccion_argentina,
                f"livepass_dashboard_{evento_nombre.replace(' ', '_')}",
                event_info['url'],
                False
            ))
            
            record_id = cursor.fetchone()[0]
            self.db_connection.commit()
            cursor.close()
            
            self.logger.info(f"✅ Datos de '{evento_nombre}' guardados exitosamente en la BD (ID: {record_id})")
            print(f"💾 GUARDADO EN BD: {json_individual['artista']} - {json_individual['venue']} - ID: {record_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error guardando evento en la base de datos: {e}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
    
    def save_data_to_database(self, all_event_data):
        """Save all extracted data to database"""
        try:
            if not all_event_data:
                self.logger.warning("No hay datos para guardar")
                return

            self.logger.info(f"💾 Datos ya guardados durante la extracción individual de eventos")
            self.logger.info(f"✅ Total de eventos procesados: {len(all_event_data)}")

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
            email = "fatima.villegas@daleplay.la"
            password = "azul-cielo-lavanda"
            
            if not self.login(email, password):
                self.logger.error("Login fallido")
                return False
            
            # Obtener eventos
            events = self.get_events_list()
            if not events:
                self.logger.warning("No se encontraron eventos")
                return False
            
            self.logger.info(f"Encontrados {len(events)} eventos")
            
            # Procesar cada evento
            all_event_data = []
            for i, event in enumerate(events):
                self.logger.info(f"Procesando evento {i+1}/{len(events)}: {event['title']}")
                
                # Validar fecha del evento antes de procesar
                event_datetime, is_future_or_today = self._parse_event_date(event['date'])
                
                if not is_future_or_today:
                    self.logger.info(f"Saltando evento {i+1} - fecha pasada: {event['date']}")
                    continue
                
                # Navegar al evento
                self.driver.get(event['url'])
                time.sleep(5)
                
                # Extraer datos del dashboard
                event_data = self.extract_event_dashboard_data()
                if event_data:
                    all_event_data.append(event_data)
                    # Guardar en base de datos inmediatamente
                    self.save_single_event_to_database(event_data, event)
                else:
                    self.logger.error(f"No se pudieron extraer datos del evento {i+1}")
            
            # Guardar datos finales
            self.save_data_to_database(all_event_data)
            
            self.logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
            return True
            
        except Exception as e:
            self.logger.error(f"Error en el proceso principal: {e}")
            return False
        finally:
            self.close_driver()

def main():
    """Función principal para ejecutar el scraper"""
    scraper = LivepassScraper(headless=True)
    success = scraper.run()
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Error ejecutando el scraper")
    
    return success

if __name__ == "__main__":
    main()

def run_scraper_for_airflow():
    """Función para ejecutar el scraper desde Airflow"""
    scraper = LivepassScraper(headless=True)
    return scraper.run()
