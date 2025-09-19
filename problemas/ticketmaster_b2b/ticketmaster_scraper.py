import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import json
import time
import logging
from datetime import datetime
import os
import requests
from urllib.parse import urlparse, parse_qs

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TicketmasterB2BScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.session = requests.Session()
        self.access_token = None
        self.base_url = "https://one.ticketmaster.com"
        
    def setup_driver(self):
        """Configura undetected Chrome driver con configuración avanzada"""
        try:
            logger.info("Configurando undetected Chrome driver...")
            
            # Configuración específica para undetected-chromedriver
            options = uc.ChromeOptions()
            
            # Configuraciones básicas anti-detección
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1366,768")
            options.add_argument("--start-maximized")
            
            # Configuraciones adicionales para parecer más humano
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-extensions")
            options.add_argument("--no-first-run")
            options.add_argument("--disable-default-apps")
            options.add_argument("--disable-infobars")
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            
            # User agent realista
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36")
            
            # Configurar preferencias
            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.images": 1,
                "profile.default_content_setting_values.media_stream_mic": 2,
                "profile.default_content_setting_values.media_stream_camera": 2,
                "profile.managed_default_content_settings.geolocation": 2
            }
            options.add_experimental_option("prefs", prefs)
            
            # Crear driver con undetected-chromedriver
            self.driver = uc.Chrome(
                options=options,
                version_main=139,  # Especificar versión de Chrome 139
                driver_executable_path=None,  # Auto-descargar
                browser_executable_path=None,  # Usar Chrome por defecto
                headless=False,  # Mostrar navegador
                use_subprocess=True,  # Usar subprocess para mayor stealth
                debug=False  # Sin debug
            )
            
            # Configurar timeouts
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(10)
            self.wait = WebDriverWait(self.driver, 30)
            
            # Scripts adicionales anti-detección después de crear el driver
            self.driver.execute_script("""
                // Configurar navigator properties
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['es-ES', 'es', 'en-US', 'en']
                });
                
                // Configurar plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => ({
                        length: 3,
                        0: {name: 'Chrome PDF Plugin'},
                        1: {name: 'Chrome PDF Viewer'},
                        2: {name: 'Native Client'}
                    })
                });
                
                // Configurar chrome object
                if (!window.chrome) {
                    window.chrome = {
                        runtime: {},
                        app: {isInstalled: false}
                    };
                }
                
                // Configurar permissions
                Object.defineProperty(navigator, 'permissions', {
                    get: () => ({
                        query: async () => ({state: 'granted'})
                    })
                });
                
                console.log('🔒 Undetected Chrome configurado exitosamente');
            """)
            
            logger.info("✅ Undetected Chrome driver configurado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error configurando undetected Chrome driver: {e}")
            return False
    
    def login_oauth(self, username, password):
        """Realiza login OAuth2 en Ticketmaster B2B"""
        try:
            # Usar la URL de OAuth2 completa que funcionó anteriormente
            oauth_url = "https://b2bid-login.ticketmaster.com/oauth2/aus1ubr9idCrHoAOq697/v1/authorize?response_type=code&client_id=0ob7nisx9eQYkJZsb2e5&scope=profile%20openid%20email&state=%F0%9F%8C%90H4sIAAAAAAAA_w3IMQ6AIAwF0Lt0NvzEkds02AhBaFPqZLy7vvE9ZJQJbAYXU49FG_lfNcJWBnRKila6xOAV4qnowKVnm1C-o-4oegi0B9P7AVrewhBOAAAA&redirect_uri=https:%2F%2Fone.ticketmaster.com%2Flogin%2Foauth2%2Fcode%2Fokta"
            
            logger.info("Navegando a página de login OAuth2")
            self.driver.get(oauth_url)
            time.sleep(10)  # Tiempo extra para cargar
            
            # Verificar si la página cargó correctamente
            page_source = self.driver.page_source
            current_url = self.driver.current_url
            
            # Verificar mensajes de error comunes
            if "caducado" in page_source.lower() or "expired" in page_source.lower():
                logger.warning("Página expirada detectada, reintentando...")
                self.driver.refresh()
                time.sleep(10)
                page_source = self.driver.page_source
            
            # Verificar si necesitamos seleccionar mercado
            if "seleccionar mercado" in page_source.lower() or "market selector" in page_source.lower():
                logger.info("Selector de mercado detectado, buscando España...")
                try:
                    # Buscar el dropdown de mercado
                    market_dropdown = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "ng-select")))
                    market_dropdown.click()
                    time.sleep(3)
                    
                    # Buscar España en las opciones disponibles
                    spain_found = False
                    try:
                        # Buscar todas las opciones disponibles
                        options = self.driver.find_elements(By.CSS_SELECTOR, ".ng-option")
                        logger.info(f"Encontradas {len(options)} opciones de mercado")
                        
                        for option in options:
                            option_text = option.text.strip()
                            if "spain" in option_text.lower() or "españa" in option_text.lower():
                                logger.info(f"España encontrada: {option_text}")
                                option.click()
                                spain_found = True
                                time.sleep(2)
                                break
                        
                        if not spain_found:
                            logger.warning("España no encontrada en las opciones disponibles")
                            # Mostrar las primeras opciones para debug
                            for i, option in enumerate(options[:5]):
                                logger.info(f"Opción {i+1}: {option.text.strip()}")
                            
                            # Cerrar el dropdown haciendo clic fuera
                            self.driver.find_element(By.TAG_NAME, "body").click()
                            time.sleep(1)
                        
                    except Exception as e:
                        logger.warning(f"Error buscando España: {e}")
                        # Cerrar el dropdown haciendo clic fuera
                        try:
                            self.driver.find_element(By.TAG_NAME, "body").click()
                            time.sleep(1)
                        except:
                            pass
                    
                    # Buscar y hacer clic en el botón "Siguiente"
                    try:
                        next_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//tm1-button[contains(text(), 'Siguiente')]")))
                        # Scroll al botón para asegurar que esté visible
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                        time.sleep(1)
                        next_button.click()
                        time.sleep(10)
                        current_url = self.driver.current_url
                        page_source = self.driver.page_source
                        logger.info(f"Después de selector de mercado - URL: {current_url}")
                        
                        if spain_found:
                            logger.info("✅ España seleccionada exitosamente")
                        else:
                            logger.info("⚠️ Continuando con mercado por defecto")
                            
                    except Exception as e:
                        logger.warning(f"Error haciendo clic en 'Siguiente': {e}")
                        # Intentar con JavaScript
                        try:
                            next_button = self.driver.find_element(By.XPATH, "//tm1-button[contains(text(), 'Siguiente')]")
                            self.driver.execute_script("arguments[0].click();", next_button)
                            time.sleep(10)
                            logger.info("Botón 'Siguiente' clickeado con JavaScript")
                        except Exception as e2:
                            logger.error(f"Error con JavaScript también: {e2}")
                            
                except Exception as e:
                    logger.warning(f"Error general manejando selector de mercado: {e}")
            
            # Verificar si hay error 404 después de seleccionar mercado
            if "error" in page_source.lower() and "404" in page_source:
                logger.warning("Error 404 detectado después de selección de mercado, recargando...")
                try:
                    # Recargar la página y continuar con mercado por defecto
                    self.driver.get(oauth_url)
                    time.sleep(10)
                    
                    # Verificar si aparece el selector de mercado nuevamente
                    page_source = self.driver.page_source
                    if "seleccionar mercado" in page_source.lower() or "market selector" in page_source.lower():
                        logger.info("Selector de mercado detectado tras recarga, continuando con mercado por defecto...")
                        try:
                            # Solo hacer clic en "Siguiente" sin cambiar mercado
                            next_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//tm1-button[contains(text(), 'Siguiente')]")))
                            next_button.click()
                            time.sleep(10)
                            current_url = self.driver.current_url
                            page_source = self.driver.page_source
                            logger.info(f"Después de recarga con mercado por defecto - URL: {current_url}")
                        except Exception as e:
                            logger.warning(f"Error tras recarga: {e}")
                    
                    # Verificar nuevamente si hay error 404
                    if "error" in page_source.lower() and "404" in page_source:
                        logger.error("Error 404 persiste después de recarga")
                        return False
                        
                except Exception as e:
                    logger.error(f"Error durante recarga: {e}")
                    return False
            
            logger.info(f"URL actual: {current_url}")
            
            # Buscar campos de login con múltiples selectores
            try:
                # Paso 1: Ingresar el identificador de usuario
                logger.info("Buscando campo de identificador...")
                
                identifier_field = None
                # Intentar múltiples selectores para el campo de usuario
                selectors = [
                    (By.NAME, "identifier"),
                    (By.ID, "identifier"),
                    (By.CSS_SELECTOR, "input[type='text']"),
                    (By.CSS_SELECTOR, "input[placeholder*='usuario']"),
                    (By.CSS_SELECTOR, "input[placeholder*='Usuario']"),
                    (By.CSS_SELECTOR, ".o-form-input-name-identifier input")
                ]
                
                for selector_type, selector_value in selectors:
                    try:
                        identifier_field = self.wait.until(EC.presence_of_element_located((selector_type, selector_value)))
                        logger.info(f"Campo encontrado con selector: {selector_value}")
                        break
                    except:
                        continue
                
                if not identifier_field:
                    logger.error("No se pudo encontrar el campo de identificador")
                    return False
                
                logger.info("Ingresando identificador de usuario")
                identifier_field.clear()
                identifier_field.send_keys(username)
                
                # Hacer clic en "Siguiente" con múltiples selectores
                next_button = None
                next_selectors = [
                    (By.XPATH, "//input[@value='Siguiente']"),
                    (By.XPATH, "//button[contains(text(), 'Siguiente')]"),
                    (By.CSS_SELECTOR, "input[type='submit']"),
                    (By.CSS_SELECTOR, "button[type='submit']"),
                    (By.XPATH, "//input[@data-type='save']")
                ]
                
                for selector_type, selector_value in next_selectors:
                    try:
                        next_button = self.wait.until(EC.element_to_be_clickable((selector_type, selector_value)))
                        logger.info(f"Botón 'Siguiente' encontrado con selector: {selector_value}")
                        break
                    except:
                        continue
                
                if next_button:
                    next_button.click()
                    time.sleep(5)
                else:
                    logger.warning("No se encontró botón 'Siguiente', continuando...")
                
                # Paso 2: Ingresar la contraseña
                logger.info("Buscando campo de contraseña...")
                
                password_field = None
                password_selectors = [
                    (By.NAME, "credentials.passcode"),
                    (By.ID, "credentials.passcode"),
                    (By.CSS_SELECTOR, "input[type='password']"),
                    (By.CSS_SELECTOR, ".o-form-input-name-credentials\\.passcode input")
                ]
                
                for selector_type, selector_value in password_selectors:
                    try:
                        password_field = self.wait.until(EC.presence_of_element_located((selector_type, selector_value)))
                        logger.info(f"Campo contraseña encontrado con selector: {selector_value}")
                        break
                    except:
                        continue
                
                if not password_field:
                    logger.error("No se pudo encontrar el campo de contraseña")
                    return False
                
                logger.info("Ingresando contraseña")
                password_field.clear()
                password_field.send_keys(password)
                
                # Hacer clic en "Verificar" con múltiples selectores
                verify_button = None
                verify_selectors = [
                    (By.XPATH, "//input[@value='Verificar']"),
                    (By.XPATH, "//button[contains(text(), 'Verificar')]"),
                    (By.CSS_SELECTOR, "input[type='submit']"),
                    (By.CSS_SELECTOR, "button[type='submit']"),
                    (By.XPATH, "//input[@data-type='save']")
                ]
                
                for selector_type, selector_value in verify_selectors:
                    try:
                        verify_button = self.wait.until(EC.element_to_be_clickable((selector_type, selector_value)))
                        logger.info(f"Botón 'Verificar' encontrado con selector: {selector_value}")
                        break
                    except:
                        continue
                
                if verify_button:
                    verify_button.click()
                else:
                    logger.warning("No se encontró botón 'Verificar', intentando enviar formulario...")
                    # Intentar enviar formulario con Enter
                    password_field.send_keys('\n')
                
                # Esperar a que se complete la autenticación OAuth2
                logger.info("Esperando completar autenticación OAuth2...")
                time.sleep(15)
                
                # Verificar si llegamos a la página de destino
                current_url = self.driver.current_url
                logger.info(f"URL actual después de login: {current_url}")
                
                # Criterios de éxito más amplios
                success_indicators = [
                    "one.ticketmaster.com" in current_url,
                    "dashboard" in current_url.lower(),
                    "home" in current_url.lower() and "login" not in current_url.lower()
                ]
                
                if any(success_indicators):
                    logger.info("Login OAuth2 exitoso")
                    
                    # Extraer cookies de sesión para requests
                    cookies = self.driver.get_cookies()
                    for cookie in cookies:
                        self.session.cookies.set(cookie['name'], cookie['value'])
                    
                    return True
                else:
                    logger.error("Login OAuth2 fallido")
                    logger.info(f"URL final: {current_url}")
                    return False
                    
            except Exception as e:
                logger.error(f"Error durante el proceso de login: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error en login OAuth2: {e}")
            return False
    
    def navigate_to_dashboard(self):
        """Navega a la página de reportes específica"""
        try:
            # Navegar directamente a la página de reportes
            reports_url = f"{self.base_url}/app/reports"
            logger.info(f"Navegando a página de reportes: {reports_url}")
            self.driver.get(reports_url)
            
            # Dar 15 segundos para que cargue la página de reportes
            logger.info("Esperando 15 segundos para que cargue la página de reportes...")
            time.sleep(15)
            
            # Verificar si la página cargó correctamente
            current_url = self.driver.current_url
            page_title = self.driver.title
            logger.info(f"URL actual: {current_url}")
            logger.info(f"Título de página: {page_title}")
            
            # Verificar si hay problemas de cookies o timeout
            page_source = self.driver.page_source
            if "cookies are required" in page_source.lower():
                logger.warning("Problema de cookies detectado, refrescando página...")
                self.driver.refresh()
                time.sleep(10)
            elif "page has timed out" in page_source.lower():
                logger.warning("Timeout de página detectado, refrescando...")
                self.driver.refresh()
                time.sleep(10)
            
            # Dar 40 segundos adicionales en la página como solicitado
            logger.info("Esperando 40 segundos adicionales en la página de reportes...")
            time.sleep(40)
            
            # Ahora realizar las acciones específicas en la página de reportes
            logger.info("Realizando acciones en la página de reportes...")
            
            try:
                # Paso 1: Buscar y hacer clic en el checkbox de eventos
                logger.info("Buscando checkbox de eventos...")
                checkbox_selectors = [
                    "input.evt_checkMonth",
                    "input[ng-model='eventGroup.selected']",
                    "input[type='checkbox'][class*='evt_checkMonth']",
                    ".evt_checkMonth"
                ]
                
                checkbox_found = False
                for selector in checkbox_selectors:
                    try:
                        checkbox = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                        logger.info(f"Checkbox encontrado con selector: {selector}")
                        
                        # Verificar si ya está seleccionado
                        if not checkbox.is_selected():
                            checkbox.click()
                            logger.info("✅ Checkbox de eventos seleccionado")
                        else:
                            logger.info("✅ Checkbox ya estaba seleccionado")
                        
                        checkbox_found = True
                        time.sleep(2)
                        break
                    except:
                        continue
                
                if not checkbox_found:
                    logger.warning("No se pudo encontrar el checkbox de eventos")
                
                # Paso 2: Buscar y hacer clic en el botón "Ejecutar listado"
                logger.info("Buscando botón 'Ejecutar listado'...")
                button_selectors = [
                    "#runReport",
                    "button[ng-click='runSelectedReport()']",
                    "button[id='runReport']",
                    "button:contains('Ejecutar listado')",
                    "button .ng-scope:contains('Ejecutar listado')"
                ]
                
                button_found = False
                for selector in button_selectors:
                    try:
                        if "contains" in selector:
                            # Usar XPath para selectores con contains
                            button = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[contains(., 'Ejecutar listado')]")))
                        else:
                            button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                        
                        logger.info(f"Botón 'Ejecutar listado' encontrado con selector: {selector}")
                        
                        # Verificar si el botón está habilitado
                        if not button.get_attribute("disabled"):
                            button.click()
                            logger.info("✅ Botón 'Ejecutar listado' clickeado")
                            button_found = True
                            time.sleep(5)  # Esperar a que se procese
                            break
                        else:
                            logger.warning("Botón 'Ejecutar listado' está deshabilitado")
                    except:
                        continue
                
                if not button_found:
                    logger.warning("No se pudo encontrar o hacer clic en el botón 'Ejecutar listado'")
                    # Intentar con XPath más general
                    try:
                        button = self.driver.find_element(By.XPATH, "//button[contains(@class, 'btn-primary')]")
                        if button and not button.get_attribute("disabled"):
                            button.click()
                            logger.info("✅ Botón principal clickeado como alternativa")
                            time.sleep(5)
                    except:
                        logger.warning("No se pudo hacer clic en ningún botón alternativo")
                
                # Esperar a que se genere el reporte
                logger.info("Esperando a que se genere el reporte...")
                time.sleep(10)
                
            except Exception as e:
                logger.warning(f"Error realizando acciones en reportes: {e}")
            
            logger.info("Navegación y acciones en reportes completadas")
            return True
        except Exception as e:
            logger.error(f"Error navegando a reportes: {e}")
            return False
    
    def get_events_data(self):
        """Obtiene datos de eventos disponibles"""
        try:
            logger.info("Obteniendo datos de eventos...")
            
            # Buscar sección de eventos
            events_data = []
            
            # Intentar diferentes selectores para encontrar eventos
            event_selectors = [
                "//div[contains(@class, 'event')]",
                "//div[contains(@class, 'show')]",
                "//div[contains(@class, 'listing')]",
                "//tr[contains(@class, 'event-row')]",
                "//div[contains(@data-testid, 'event')]"
            ]
            
            events_found = False
            for selector in event_selectors:
                try:
                    events = self.driver.find_elements(By.XPATH, selector)
                    if events:
                        events_found = True
                        logger.info(f"Encontrados {len(events)} eventos con selector: {selector}")
                        
                        for event in events:
                            try:
                                event_data = self.extract_event_details(event)
                                if event_data:
                                    events_data.append(event_data)
                            except Exception as e:
                                logger.warning(f"Error procesando evento individual: {e}")
                                continue
                        break
                except:
                    continue
            
            if not events_found:
                # Intentar extraer datos de tablas
                events_data = self.extract_table_data()
            
            logger.info(f"Total de eventos extraídos: {len(events_data)}")
            return events_data
            
        except Exception as e:
            logger.error(f"Error obteniendo eventos: {e}")
            return []
    
    def extract_event_details(self, event_element):
        """Extrae detalles de un elemento de evento"""
        try:
            event_data = {
                'timestamp': datetime.now().isoformat()
            }
            
            # Intentar extraer título/nombre del evento
            title_selectors = [".//h1", ".//h2", ".//h3", ".//h4", 
                             ".//span[contains(@class, 'title')]",
                             ".//div[contains(@class, 'name')]"]
            
            for selector in title_selectors:
                try:
                    title_element = event_element.find_element(By.XPATH, selector)
                    event_data['title'] = title_element.text.strip()
                    break
                except:
                    continue
            
            # Intentar extraer fecha
            date_selectors = [".//span[contains(@class, 'date')]",
                            ".//div[contains(@class, 'date')]",
                            ".//time"]
            
            for selector in date_selectors:
                try:
                    date_element = event_element.find_element(By.XPATH, selector)
                    event_data['date'] = date_element.text.strip()
                    break
                except:
                    continue
            
            # Intentar extraer venue/lugar
            venue_selectors = [".//span[contains(@class, 'venue')]",
                             ".//div[contains(@class, 'venue')]",
                             ".//span[contains(@class, 'location')]"]
            
            for selector in venue_selectors:
                try:
                    venue_element = event_element.find_element(By.XPATH, selector)
                    event_data['venue'] = venue_element.text.strip()
                    break
                except:
                    continue
            
            # Intentar extraer precios
            price_selectors = [".//span[contains(@class, 'price')]",
                             ".//div[contains(@class, 'price')]",
                             ".//span[contains(text(), '$')]"]
            
            for selector in price_selectors:
                try:
                    price_element = event_element.find_element(By.XPATH, selector)
                    event_data['price'] = price_element.text.strip()
                    break
                except:
                    continue
            
            # Intentar extraer estado
            status_selectors = [".//span[contains(@class, 'status')]",
                              ".//div[contains(@class, 'status')]"]
            
            for selector in status_selectors:
                try:
                    status_element = event_element.find_element(By.XPATH, selector)
                    event_data['status'] = status_element.text.strip()
                    break
                except:
                    continue
            
            # Solo devolver si tenemos al menos el título
            if 'title' in event_data and event_data['title']:
                return event_data
            else:
                return None
                
        except Exception as e:
            logger.warning(f"Error extrayendo detalles del evento: {e}")
            return None
    
    def extract_table_data(self):
        """Extrae datos de tablas en la página"""
        try:
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            all_data = []
            
            for table in tables:
                try:
                    # Obtener encabezados
                    headers = []
                    header_cells = table.find_elements(By.XPATH, ".//thead//th | .//tr[1]//th")
                    for cell in header_cells:
                        headers.append(cell.text.strip())
                    
                    if not headers:
                        # Si no hay encabezados específicos, usar la primera fila
                        first_row = table.find_element(By.XPATH, ".//tr[1]")
                        cells = first_row.find_elements(By.TAG_NAME, "td")
                        for i, cell in enumerate(cells):
                            headers.append(f"column_{i+1}")
                    
                    # Obtener filas de datos
                    data_rows = table.find_elements(By.XPATH, ".//tbody//tr | .//tr[position()>1]")
                    
                    for row in data_rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if cells:
                            row_data = {
                                'timestamp': datetime.now().isoformat()
                            }
                            for i, cell in enumerate(cells):
                                if i < len(headers):
                                    row_data[headers[i]] = cell.text.strip()
                            all_data.append(row_data)
                
                except Exception as e:
                    logger.warning(f"Error procesando tabla: {e}")
                    continue
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de tabla: {e}")
            return []
    
    def get_sales_reports(self):
        """Obtiene reportes de ventas si están disponibles"""
        try:
            # Buscar enlaces de reportes
            report_links = [
                "//a[contains(@href, 'report')]",
                "//a[contains(@href, 'sales')]",
                "//a[contains(text(), 'Report')]",
                "//a[contains(text(), 'Sales')]"
            ]
            
            for link_selector in report_links:
                try:
                    report_link = self.driver.find_element(By.XPATH, link_selector)
                    report_link.click()
                    time.sleep(5)
                    
                    # Extraer datos de reportes
                    report_data = self.extract_table_data()
                    if report_data:
                        return report_data
                        
                except:
                    continue
            
            return []
            
        except Exception as e:
            logger.warning(f"No se pudieron obtener reportes de ventas: {e}")
            return []
    
    def save_data(self, data, data_type="events"):
        """Guarda los datos en un archivo JSON"""
        if not os.path.exists('jsonticketmaster'):
            os.makedirs('jsonticketmaster')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"jsonticketmaster/ticketmaster_b2b_{data_type}_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Datos guardados en {filename}")
        return filename
    
    def scrape_complete(self, username, password):
        """Proceso completo de scraping"""
        logger.info("Iniciando scraping de Ticketmaster B2B")
        
        if not self.setup_driver():
            return None
        
        try:
            # Login OAuth2
            if not self.login_oauth(username, password):
                logger.error("No se pudo realizar login OAuth2")
                return None
            
            # Navegar al dashboard
            if not self.navigate_to_dashboard():
                logger.error("No se pudo navegar al dashboard")
                # Continuar de todas formas, podríamos estar en la página correcta
            
            # Obtener datos de eventos
            events_data = self.get_events_data()
            
            # Obtener reportes de ventas
            sales_data = self.get_sales_reports()
            
            # Compilar todos los datos
            complete_data = {
                'events': events_data,
                'sales_reports': sales_data,
                'timestamp': datetime.now().isoformat(),
                'total_events': len(events_data),
                'total_sales_records': len(sales_data),
                'platform': 'Ticketmaster B2B',
                'login_url': 'https://b2bid-login.ticketmaster.com'
            }
            
            # Guardar datos completos
            filename = self.save_data(complete_data, "complete")
            
            # Guardar eventos por separado si hay datos
            if events_data:
                self.save_data(events_data, "events")
            
            # Guardar reportes por separado si hay datos
            if sales_data:
                self.save_data(sales_data, "sales")
            
            logger.info(f"Scraping completado. {len(events_data)} eventos y {len(sales_data)} registros de ventas encontrados")
            return complete_data
            
        except Exception as e:
            logger.error(f"Error en scraping: {e}")
            return None
        finally:
            if self.driver:
                # Guardar HTML de debug antes de cerrar
                try:
                    with open('debug_ticketmaster_final.html', 'w', encoding='utf-8') as f:
                        f.write(self.driver.page_source)
                    logger.info("HTML de debug guardado en debug_ticketmaster_final.html")
                except:
                    pass
                    
                self.driver.quit()

def main():
    scraper = TicketmasterB2BScraper()
    
    # Credenciales proporcionadas
    username = "T1espaventa114"
    password = "Duki2025"
    
    # Ejecutar scraping
    results = scraper.scrape_complete(username, password)
    
    if results:
        print(f"Scraping completado exitosamente.")
        print(f"Eventos encontrados: {results['total_events']}")
        print(f"Registros de ventas: {results['total_sales_records']}")
    else:
        print("Error en el scraping.")

if __name__ == "__main__":
    main()
