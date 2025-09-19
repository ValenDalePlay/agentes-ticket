 #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EntradaUno Scraper
Automatiza la extracción de datos del Histórico de Ventas desde el sistema EntradaUno
"""

import time
import json
import logging
import re
import random
from datetime import datetime, timezone, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager
import os
import psycopg2
from database_config import get_database_connection

class EntradaUnoScraper:
    def __init__(self, headless=True, test_mode=False):
        self.setup_logging()
        self.driver = None
        self.wait = None
        self.headless = headless
        self.test_mode = test_mode
        self.base_url = "https://bo.entradauno.com"
        self.login_url = f"{self.base_url}/Home/Login?ReturnUrl=/Reporte/General/HistoricoDeVentas"
        self.target_url = f"{self.base_url}/Reporte/General/HistoricoDeVentas"
        
        # Credenciales
        self.username = "fLauria"
        self.password = "lauria2021"
        
        # Configurar carpeta de salida
        if self.test_mode:
            self.output_dir = "test_output"
            os.makedirs(self.output_dir, exist_ok=True)
        else:
            self.output_dir = "/tmp"
        
        # Inicializar conexión a base de datos
        self.db_connection = None
        self.final_data = []
        self.setup_database_connection()
        
    def setup_logging(self):
        """Configurar logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('entradauno_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== INICIANDO SCRAPER DE ENTRADAUNO ===")
    
    def setup_database_connection(self):
        """Establece conexión con la base de datos"""
        if self.test_mode:
            self.logger.info("🧪 MODO TEST: Saltando conexión a base de datos")
            return
            
        try:
            self.db_connection = get_database_connection()
            if self.db_connection:
                self.logger.info("✅ Conexión exitosa! Hora actual: " + str(datetime.now(timezone.utc)))
            else:
                self.logger.error("❌ No se pudo establecer conexión a la base de datos")
        except Exception as e:
            self.logger.error(f"Error en setup_database_connection: {e}")
    
    def parse_number_with_dots(self, number_str):
        """Parsea números que pueden tener puntos como separadores de miles"""
        try:
            if not number_str or number_str == '' or number_str == '-':
                return 0
            
            # Remover $ y espacios
            cleaned = number_str.replace('$', '').replace(' ', '').strip()
            
            # Si tiene puntos, asumir que son separadores de miles
            if '.' in cleaned:
                # Remover puntos y convertir a int
                return int(cleaned.replace('.', ''))
            else:
                return int(cleaned)
                
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Error parseando número '{number_str}': {e}")
            return 0
    
    def parse_function_code(self, function_code):
        """Parsea el código de función para extraer artista, fecha y hora"""
        try:
            if not function_code or function_code == '':
                return None
            
            self.logger.info(f"🔍 Parseando código de función: '{function_code}'")
            
            # Buscar el patrón: LETRAS + FECHA + HORA
            import re
            
            # Patrón para extraer: letras + 8 dígitos (fecha) + 4 dígitos (hora)
            pattern = r'^([A-Z]+)(\d{8})(\d{4})$'
            match = re.match(pattern, function_code)
            
            if match:
                artista_code = match.group(1)
                fecha_code = match.group(2)
                hora_code = match.group(3)
                
                # Parsear fecha: YYYYMMDD
                if len(fecha_code) == 8:
                    year = fecha_code[:4]
                    month = fecha_code[4:6]
                    day = fecha_code[6:8]
                    fecha_formateada = f"{year}/{month}/{day}"
                else:
                    fecha_formateada = fecha_code
                
                # Parsear hora: HHMM
                if len(hora_code) == 4:
                    hour = hora_code[:2]
                    minute = hora_code[2:4]
                    hora_formateada = f"{hour}:{minute}hs"
                else:
                    hora_formateada = f"{hora_code}hs"
                
                # Mapear códigos de artistas conocidos
                artistas_map = {
                    'ALSA': 'Alejandro Sanz',
                    'COLD': 'Coldplay',
                    'U2': 'U2',
                    'MADN': 'Madonna',
                    'BEAT': 'The Beatles'
                    # Agregar más mapeos según sea necesario
                }
                
                artista_nombre = artistas_map.get(artista_code, artista_code)
                
                resultado = {
                    'codigo_completo': function_code,
                    'artista_codigo': artista_code,
                    'artista_nombre': artista_nombre,
                    'fecha_codigo': fecha_code,
                    'fecha_formateada': fecha_formateada,
                    'hora_codigo': hora_code,
                    'hora_formateada': hora_formateada,
                    'fecha_hora_completa': f"{fecha_formateada} {hora_formateada}"
                }
                
                self.logger.info(f"✅ Código parseado: {artista_nombre} - {fecha_formateada} {hora_formateada}")
                return resultado
            else:
                self.logger.warning(f"⚠️ No se pudo parsear el código: '{function_code}'")
                return {
                    'codigo_completo': function_code,
                    'artista_codigo': 'DESCONOCIDO',
                    'artista_nombre': 'Artista Desconocido',
                    'fecha_codigo': 'DESCONOCIDA',
                    'fecha_formateada': 'Fecha Desconocida',
                    'hora_codigo': 'DESCONOCIDA',
                    'hora_formateada': 'Hora Desconocida',
                    'fecha_hora_completa': 'Fecha/Hora Desconocida'
                }
                
        except Exception as e:
            self.logger.error(f"Error parseando código de función '{function_code}': {e}")
            return None
        
    def setup_driver(self):
        """Configurar el driver de Chrome con evasión de bots avanzada"""
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
            
            self.wait = WebDriverWait(self.driver, 20)
            self.logger.info("Driver de Chrome configurado exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error configurando ChromeDriverManager: {e}")
            self.logger.info("Intentando usar ChromeDriver del sistema...")
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                self.wait = WebDriverWait(self.driver, 20)
                self.logger.info("Driver de Chrome configurado exitosamente")
                return True
            except Exception as e2:
                self.logger.error(f"Error al configurar el driver: {e2}")
                return False
    
    def close(self):
        """Cierra el driver del navegador y la conexión a la base de datos"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Driver cerrado")
        if self.db_connection:
            self.db_connection.close()
            self.logger.info("Conexión a base de datos cerrada")
            
    def login(self):
        """Realizar login en EntradaUno"""
        try:
            self.logger.info("Iniciando proceso de login")
            self.driver.get(self.login_url)
            time.sleep(3)
            
            # Hacer clic en "Siguiente" si existe
            try:
                siguiente_btn = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-login-default.btn-login.ripple"))
                )
                siguiente_btn.click()
                self.logger.info("Botón 'Siguiente' clickeado")
                time.sleep(2)
            except TimeoutException:
                self.logger.info("Botón 'Siguiente' no encontrado, continuando...")
            
            # Esperar a que aparezcan los campos de login
            time.sleep(2)
            
            # Llenar usuario
            username_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "clogin"))
            )
            username_field.clear()
            username_field.send_keys(self.username)
            self.logger.info("Usuario ingresado")
            
            # Llenar contraseña
            password_field = self.driver.find_element(By.ID, "cclave")
            password_field.clear()
            password_field.send_keys(self.password)
            self.logger.info("Contraseña ingresada")
            
            # Hacer clic en "Ingresar"
            login_btn = self.driver.find_element(By.ID, "login")
            login_btn.click()
            self.logger.info("Botón 'Ingresar' clickeado")
            
            # Esperar a que cargue la página después del login
            time.sleep(5)
            
            # Verificar si el login fue exitoso
            if "Login" not in self.driver.current_url:
                self.logger.info("Login exitoso")
                return True
            else:
                self.logger.error("Login falló")
                return False
                
        except Exception as e:
            self.logger.error(f"Error durante el login: {e}")
            return False
            
    def navigate_to_historico_ventas(self):
        """Navegar a la página de Histórico de Ventas"""
        try:
            self.logger.info("Navegando a página de Histórico de Ventas")
            self.driver.get(self.target_url)
            time.sleep(5)
            
            # Verificar que estamos en la página correcta
            if "HistoricoDeVentas" in self.driver.current_url:
                self.logger.info("Navegación a Histórico de Ventas exitosa")
                return True
            else:
                self.logger.error("Error navegando a Histórico de Ventas")
                return False
                
        except Exception as e:
            self.logger.error(f"Error navegando a Histórico de Ventas: {e}")
            return False
    
    def click_buscar_button(self):
        """Hacer clic en el botón Buscar para obtener resultados"""
        try:
            self.logger.info("Buscando botón 'Buscar'...")
            
            # Buscar el botón Buscar con diferentes selectores
            buscar_selectors = [
                "#loadButton",
                "button[type='submit']",
                "input[type='submit']",
                "button:contains('Buscar')",
                "input[value='Buscar']",
                ".btn-buscar",
                "#btnBuscar",
                "button.btn-primary",
                "button.btn"
            ]
            
            buscar_button = None
            for selector in buscar_selectors:
                try:
                    if ":contains" in selector:
                        # Para selectores con :contains, usar XPath
                        xpath = f"//button[contains(text(), 'Buscar')]"
                        buscar_button = self.driver.find_element(By.XPATH, xpath)
                    else:
                        buscar_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if buscar_button and buscar_button.is_displayed():
                        self.logger.info(f"Botón Buscar encontrado con selector: {selector}")
                        break
                except:
                    continue
            
            if not buscar_button:
                # Buscar por texto
                try:
                    buscar_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Buscar')] | //input[@value='Buscar']")
                except:
                    pass
            
            if buscar_button:
                # Hacer clic en el botón
                self.driver.execute_script("arguments[0].click();", buscar_button)
                self.logger.info("✅ Botón 'Buscar' clickeado exitosamente")
                time.sleep(15)  # Esperar más tiempo para que carguen los resultados
                
                # Tomar screenshot después de buscar
                if self.test_mode:
                    screenshot_path = os.path.join(self.output_dir, "after_search.png")
                    self.driver.save_screenshot(screenshot_path)
                    self.logger.info(f"📸 Screenshot después de buscar guardado en: {screenshot_path}")
                
                return True
            else:
                self.logger.error("❌ No se encontró el botón 'Buscar'")
                return False
                
        except Exception as e:
            self.logger.error(f"Error haciendo clic en botón Buscar: {e}")
            return False
            
    def debug_page_structure(self):
        """Debug para inspeccionar la estructura de la página"""
        try:
            self.logger.info("=== 🔍 DEBUG: Inspeccionando estructura de página ===")
            self.logger.info(f"🌐 URL actual: {self.driver.current_url}")
            self.logger.info(f"📄 Título de la página: {self.driver.title}")
            
            # Buscar todos los dropdowns
            dropdowns = self.driver.find_elements(By.CSS_SELECTOR, ".dx-selectbox")
            self.logger.info(f"📋 Encontrados {len(dropdowns)} dropdowns")
            
            # Buscar selectores específicos
            establecimientos_elem = self.driver.find_elements(By.ID, "selectEstablecimiento")
            self.logger.info(f"🏢 Elemento selectEstablecimiento encontrado: {len(establecimientos_elem) > 0}")
            
            if establecimientos_elem:
                elem = establecimientos_elem[0]
                self.logger.info(f"🏷️ Clases del elemento: {elem.get_attribute('class')}")
                self.logger.info(f"✅ Estado habilitado: {elem.is_enabled()}")
                
                # Verificar si está deshabilitado
                if "dx-state-disabled" in elem.get_attribute('class'):
                    self.logger.warning("⚠️ El dropdown de establecimientos está deshabilitado")
                    
            # Buscar overlays existentes
            overlays = self.driver.find_elements(By.CSS_SELECTOR, ".dx-overlay")
            self.logger.info(f"🔄 Encontrados {len(overlays)} overlays")
            
            # Verificar contenido de la página
            page_text = self.driver.find_element(By.TAG_NAME, "body").text
            if "Solo Vigentes" in page_text:
                self.logger.info("✅ Texto 'Solo Vigentes' encontrado en la página")
            if "Establecimiento" in page_text:
                self.logger.info("✅ Texto 'Establecimiento' encontrado en la página")
            if "Histórico" in page_text or "Historico" in page_text:
                self.logger.info("✅ Texto 'Histórico' encontrado en la página")
            if "Ventas" in page_text:
                self.logger.info("✅ Texto 'Ventas' encontrado en la página")
                
            # Buscar tablas de datos
            main_table = self.driver.find_elements(By.ID, "gridReportes")
            summary_table = self.driver.find_elements(By.ID, "gridReportesDOS")
            payment_table = self.driver.find_elements(By.ID, "gridReportesTRES")
            
            self.logger.info(f"📊 Tabla principal (gridReportes): {len(main_table) > 0}")
            self.logger.info(f"📊 Tabla resumen (gridReportesDOS): {len(summary_table) > 0}")
            self.logger.info(f"📊 Tabla pagos (gridReportesTRES): {len(payment_table) > 0}")
            
            # En modo test, guardar screenshot
            if self.test_mode:
                screenshot_path = os.path.join(self.output_dir, "debug_page_screenshot.png")
                self.driver.save_screenshot(screenshot_path)
                self.logger.info(f"📸 Screenshot guardado en: {screenshot_path}")
                
            self.logger.info("=== ✅ FIN DEBUG ===")
            
        except Exception as e:
            self.logger.error(f"Error en debug: {e}")
            
    def get_dropdown_options(self, dropdown_id):
        """Obtener opciones de un dropdown DevExtreme usando JavaScript"""
        try:
            self.logger.info(f"Obteniendo opciones para {dropdown_id}")
            
            # Usar JavaScript para obtener las opciones del dropdown
            script = f"""
            var dropdown = $("#{dropdown_id}").dxSelectBox("instance");
            if (dropdown) {{
                var dataSource = dropdown.option("dataSource");
                if (dataSource && Array.isArray(dataSource)) {{
                    return dataSource.map(function(item) {{
                        if (typeof item === 'string') {{
                            return {{ text: item, value: item }};
                        }} else if (item.text) {{
                            return {{ text: item.text, value: item.value || item.text }};
                        }} else {{
                            return {{ text: String(item), value: item }};
                        }}
                    }});
                }}
            }}
            return [];
            """
            
            options_data = self.driver.execute_script(script)
            
            if options_data:
                options = [{'text': opt['text'], 'value': opt['value']} for opt in options_data]
                self.logger.info(f"Encontradas {len(options)} opciones via JavaScript: {[opt['text'] for opt in options]}")
                return options
            
            # Si JavaScript no funciona, intentar método manual
            return self.get_dropdown_options_manual(dropdown_id)
            
        except Exception as e:
            self.logger.error(f"Error obteniendo opciones de {dropdown_id}: {e}")
            return self.get_dropdown_options_manual(dropdown_id)
            
    def get_dropdown_options_manual(self, dropdown_id):
        """Método manual para obtener opciones de dropdown"""
        try:
            # Hacer clic directamente en el input del dropdown
            dropdown_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"#{dropdown_id} .dx-texteditor-input"))
            )
            dropdown_input.click()
            time.sleep(3)
            
            # Buscar el overlay que aparece
            overlays = self.driver.find_elements(By.CSS_SELECTOR, ".dx-overlay:not(.dx-state-invisible)")
            
            options = []
            for overlay in overlays:
                try:
                    items = overlay.find_elements(By.CSS_SELECTOR, ".dx-list-item .dx-item-content")
                    for item in items:
                        text = item.get_attribute('textContent').strip()
                        if text and text != "No data to display":
                            options.append({'text': text, 'element': item.find_element(By.XPATH, "..")})
                    
                    if options:
                        break
                except:
                    continue
            
            # Cerrar dropdown
            self.driver.find_element(By.TAG_NAME, "body").click()
            time.sleep(1)
            
            self.logger.info(f"Encontradas {len(options)} opciones manualmente: {[opt['text'] for opt in options]}")
            return options
            
        except Exception as e:
            self.logger.error(f"Error en método manual para {dropdown_id}: {e}")
            return []
    
    def get_all_establecimientos(self):
        """Obtener todos los establecimientos disponibles"""
        try:
            self.logger.info("🔍 Obteniendo todos los establecimientos disponibles...")
            establecimientos = self.get_dropdown_options("selectEstablecimiento")
            
            if not establecimientos:
                self.logger.warning("⚠️ No se encontraron establecimientos, usando valores conocidos...")
                establecimientos = [
                    {'text': 'Autódromo de Rosario', 'value': '145'},
                    {'text': 'Metropolitano Rosario', 'value': '146'}
                ]
            
            self.logger.info(f"✅ Encontrados {len(establecimientos)} establecimientos")
            for i, est in enumerate(establecimientos):
                self.logger.info(f"  {i+1}. {est['text']}")
            
            return establecimientos
            
        except Exception as e:
            self.logger.error(f"Error obteniendo establecimientos: {e}")
            return []
    
    def get_all_espectaculos(self, establecimiento_text):
        """Obtener todos los espectáculos disponibles para un establecimiento"""
        try:
            self.logger.info(f"🔍 Obteniendo espectáculos para: {establecimiento_text}")
            
            # Seleccionar el establecimiento primero
            if not self.force_dropdown_selection("selectEstablecimiento", establecimiento_text):
                self.logger.error(f"No se pudo seleccionar establecimiento: {establecimiento_text}")
                return []
            
            # Esperar a que se carguen los espectáculos
            time.sleep(5)
            
            # Obtener opciones de espectáculos
            espectaculos = self.get_dropdown_options("selectEspectaculo")
            
            if not espectaculos:
                self.logger.warning("⚠️ No se encontraron espectáculos, usando valores conocidos...")
                espectaculos = [
                    {'text': 'Alejandro Sanz', 'value': '15886'},
                    {'text': 'Otro Espectáculo', 'value': '15887'}
                ]
            
            self.logger.info(f"✅ Encontrados {len(espectaculos)} espectáculos para {establecimiento_text}")
            for i, esp in enumerate(espectaculos):
                self.logger.info(f"  {i+1}. {esp['text']}")
            
            return espectaculos
            
        except Exception as e:
            self.logger.error(f"Error obteniendo espectáculos para {establecimiento_text}: {e}")
            return []
            
    def select_dropdown_option(self, dropdown_id, option_text):
        """Seleccionar una opción específica del dropdown DevExtreme"""
        try:
            self.logger.info(f"Seleccionando '{option_text}' en {dropdown_id}")
            
            # Método 1: Usar JavaScript para seleccionar directamente
            script = f"""
            var dropdown = $("#{dropdown_id}").dxSelectBox("instance");
            if (dropdown) {{
                var dataSource = dropdown.option("dataSource");
                if (dataSource && Array.isArray(dataSource)) {{
                    var option = dataSource.find(function(item) {{
                        var itemText = typeof item === 'string' ? item : item.text || String(item);
                        return itemText.includes("{option_text}") || itemText === "{option_text}";
                    }});
                    
                    if (option) {{
                        var value = typeof option === 'string' ? option : option.value || option.text;
                        dropdown.option("value", value);
                        
                        // Trigger change event
                        var changeEvent = new Event('change', {{ bubbles: true }});
                        dropdown.element()[0].dispatchEvent(changeEvent);
                        
                        return true;
                    }}
                }}
            }}
            return false;
            """
            
            result = self.driver.execute_script(script)
            
            if result:
                self.logger.info(f"Selección exitosa via JavaScript: '{option_text}' en {dropdown_id}")
                time.sleep(3)  # Esperar a que se procesen los cambios
                return True
                
            # Método 2: Selección manual si JavaScript falla
            return self.select_dropdown_option_manual(dropdown_id, option_text)
            
        except Exception as e:
            self.logger.error(f"Error seleccionando '{option_text}' en {dropdown_id}: {e}")
            return self.select_dropdown_option_manual(dropdown_id, option_text)
            
    def select_dropdown_option_manual(self, dropdown_id, option_text):
        """Método manual para seleccionar opción de dropdown"""
        try:
            # Hacer clic en el input del dropdown
            dropdown_input = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, f"#{dropdown_id} .dx-texteditor-input"))
            )
            dropdown_input.click()
            time.sleep(3)
            
            # Buscar la opción en todos los overlays visibles
            overlays = self.driver.find_elements(By.CSS_SELECTOR, ".dx-overlay:not(.dx-state-invisible)")
            
            option_element = None
            for overlay in overlays:
                try:
                    items = overlay.find_elements(By.CSS_SELECTOR, ".dx-list-item")
                    for item in items:
                        content = item.find_element(By.CSS_SELECTOR, ".dx-item-content")
                        text = content.get_attribute('textContent').strip()
                        if option_text in text or text == option_text:
                            option_element = item
                            break
                    
                    if option_element:
                        break
                except:
                    continue
            
            if option_element:
                # Hacer scroll al elemento si es necesario
                self.driver.execute_script("arguments[0].scrollIntoView(true);", option_element)
                time.sleep(1)
                
                # Intentar múltiples estrategias de clic
                clicked = False
                
                # Estrategia 1: Clic normal
                try:
                    option_element.click()
                    clicked = True
                    self.logger.info("Clic manual normal exitoso")
                except Exception as e:
                    self.logger.warning(f"Clic normal falló: {e}")
                
                # Estrategia 2: JavaScript click
                if not clicked:
                    try:
                        self.driver.execute_script("arguments[0].click();", option_element)
                        clicked = True
                        self.logger.info("Clic manual JavaScript exitoso")
                    except Exception as e:
                        self.logger.warning(f"Clic JavaScript falló: {e}")
                
                if clicked:
                    time.sleep(3)  # Esperar a que se procese la selección
                    self.logger.info(f"Selección manual exitosa: '{option_text}' en {dropdown_id}")
                    return True
                else:
                    self.logger.error(f"Todas las estrategias de clic manual fallaron para '{option_text}'")
                    return False
            else:
                self.logger.error(f"No se encontró la opción '{option_text}' en {dropdown_id}")
                # Cerrar dropdown
                try:
                    self.driver.find_element(By.TAG_NAME, "body").click()
                except:
                    pass
                return False
                
        except Exception as e:
            self.logger.error(f"Error en selección manual de '{option_text}' en {dropdown_id}: {e}")
            # Cerrar dropdown en caso de error
            try:
                self.driver.find_element(By.TAG_NAME, "body").click()
            except:
                pass
            return False
            
    def force_dropdown_selection(self, dropdown_id, option_text):
        """Método simple y directo para seleccionar dropdowns"""
        try:
            self.logger.info(f"🔍 Seleccionando '{option_text}' en {dropdown_id}")
            
            # Verificar si ya está seleccionado
            try:
                dropdown_input = self.driver.find_element(By.CSS_SELECTOR, f"#{dropdown_id} .dx-texteditor-input")
                current_value = dropdown_input.get_attribute('value')
                if current_value and option_text.lower() in current_value.lower():
                    self.logger.info(f"✅ Ya está seleccionado: '{current_value}'")
                    return True
                
                # Verificar si hay opciones seleccionadas en el overlay
                selected_items = self.driver.find_elements(By.CSS_SELECTOR, f"#{dropdown_id} .dx-overlay .dx-list-item-selected .dx-item-content")
                for item in selected_items:
                    item_text = item.text.strip()
                    if option_text.lower() in item_text.lower():
                        self.logger.info(f"✅ Opción ya seleccionada en overlay: '{item_text}'")
                        return True
                        
            except:
                pass
            
            # Método 1: Hacer clic en el dropdown y escribir directamente
            try:
                # Encontrar el input del dropdown
                dropdown_input = self.driver.find_element(By.CSS_SELECTOR, f"#{dropdown_id} .dx-texteditor-input")
                
                # Verificar si está habilitado
                if not dropdown_input.is_enabled():
                    self.logger.warning(f"⚠️ Dropdown {dropdown_id} está deshabilitado")
                    # Intentar habilitar con JavaScript
                    script = f"""
                    var dropdown = $("#{dropdown_id}").dxSelectBox("instance");
                    if (dropdown) {{
                        dropdown.option("disabled", false);
                        return true;
                    }}
                    return false;
                    """
                    result = self.driver.execute_script(script)
                    self.logger.info(f"🔧 Intentando habilitar dropdown: {result}")
                    time.sleep(2)
                
                # Hacer clic en el input usando JavaScript si está deshabilitado
                try:
                    dropdown_input.click()
                except:
                    self.driver.execute_script("arguments[0].click();", dropdown_input)
                time.sleep(1)
                
                # Limpiar y escribir el texto
                dropdown_input.clear()
                dropdown_input.send_keys(option_text)
                time.sleep(1)
                
                # Confirmar la selección con Enter
                from selenium.webdriver.common.keys import Keys
                dropdown_input.send_keys(Keys.ENTER)
                time.sleep(2)
                
                # También intentar hacer clic afuera para confirmar
                try:
                    self.driver.find_element(By.TAG_NAME, "body").click()
                    time.sleep(1)
                except:
                    pass
                
                # Verificar si se seleccionó
                current_value = dropdown_input.get_attribute('value')
                self.logger.info(f"Valor actual después de confirmar: '{current_value}'")
                
                if current_value and option_text.lower() in current_value.lower():
                    self.logger.info(f"✅ Selección exitosa: '{current_value}'")
                    return True
                else:
                    self.logger.warning(f"⚠️ No se seleccionó correctamente. Esperado: '{option_text}', Obtenido: '{current_value}'")
                    
            except Exception as e:
                self.logger.warning(f"Error con método directo: {e}")
            
            # Método 2: Usar JavaScript para forzar la selección
            try:
                script = f"""
                var dropdown = $("#{dropdown_id}").dxSelectBox("instance");
                if (dropdown) {{
                    // Intentar encontrar la opción por texto
                    var dataSource = dropdown.option("dataSource");
                    var selectedOption = null;
                    
                    if (dataSource && Array.isArray(dataSource)) {{
                        for (var i = 0; i < dataSource.length; i++) {{
                            var item = dataSource[i];
                            var itemText = typeof item === 'string' ? item : (item.text || item.displayText || String(item));
                            if (itemText.toLowerCase().includes("{option_text.lower()}")) {{
                                selectedOption = item;
                                break;
                            }}
                        }}
                    }}
                    
                    if (selectedOption) {{
                        var value = typeof selectedOption === 'string' ? selectedOption : (selectedOption.value || selectedOption.text);
                        dropdown.option("value", value);
                        
                        // Disparar eventos
                        var element = dropdown.element()[0];
                        var events = ['change', 'input', 'blur'];
                        events.forEach(function(eventType) {{
                            var event = new Event(eventType, {{ bubbles: true }});
                            element.dispatchEvent(event);
                        }});
                        
                        return true;
                    }}
                }}
                return false;
                """
                
                result = self.driver.execute_script(script)
                
                if result:
                    self.logger.info(f"✅ Selección exitosa con JavaScript")
                    time.sleep(2)
                    return True
                else:
                    self.logger.warning(f"⚠️ JavaScript no pudo seleccionar la opción")
                    
            except Exception as e:
                self.logger.warning(f"Error con JavaScript: {e}")
            
            # Método 3: Intentar con navegación por teclado
            try:
                from selenium.webdriver.common.keys import Keys
                
                dropdown_input = self.driver.find_element(By.CSS_SELECTOR, f"#{dropdown_id} .dx-texteditor-input")
                dropdown_input.click()
                time.sleep(1)
                
                # Escribir parte del texto y usar flecha abajo
                dropdown_input.clear()
                dropdown_input.send_keys(option_text[:3])  # Escribir solo las primeras letras
                time.sleep(1)
                dropdown_input.send_keys(Keys.ARROW_DOWN)
                time.sleep(1)
                dropdown_input.send_keys(Keys.ENTER)
                time.sleep(2)
                
                # Confirmar haciendo clic afuera
                try:
                    self.driver.find_element(By.TAG_NAME, "body").click()
                    time.sleep(1)
                except:
                    pass
                
                current_value = dropdown_input.get_attribute('value')
                if current_value and option_text.lower() in current_value.lower():
                    self.logger.info(f"✅ Selección exitosa con teclado: '{current_value}'")
                    return True
                    
            except Exception as e:
                self.logger.warning(f"Error con navegación por teclado: {e}")
            
            self.logger.error(f"❌ No se pudo seleccionar '{option_text}' en {dropdown_id}")
            return False
                
        except Exception as e:
            self.logger.error(f"Error en selección de dropdown: {e}")
            return False
            
    def wait_for_data_load(self, timeout=15):
        """Esperar a que los datos se carguen completamente"""
        try:
            # Esperar a que aparezca la tabla principal
            self.wait.until(
                EC.presence_of_element_located((By.ID, "gridReportes"))
            )
            
            # Esperar a que no haya indicadores de carga
            max_attempts = timeout
            for attempt in range(max_attempts):
                try:
                    # Verificar si hay indicadores de carga visibles
                    loading_indicators = self.driver.find_elements(By.CSS_SELECTOR, ".dx-loadindicator:not(.dx-state-invisible)")
                    
                    if not loading_indicators:
                        # Verificar que hay contenido en las tablas
                        main_table_rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportes .dx-datagrid-rowsview tbody tr")
                        summary_table_rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportesDOS .dx-datagrid-rowsview tbody tr")
                        
                        if len(main_table_rows) > 0 or len(summary_table_rows) > 0:
                            self.logger.info(f"Datos cargados correctamente. Filas principales: {len(main_table_rows)}, Filas resumen: {len(summary_table_rows)}")
                            time.sleep(2)  # Pequeña pausa adicional
                            return True
                        else:
                            self.logger.info(f"Intento {attempt + 1}: Esperando que aparezcan datos en las tablas...")
                    else:
                        self.logger.info(f"Intento {attempt + 1}: Detectados {len(loading_indicators)} indicadores de carga activos...")
                        
                except Exception as e:
                    self.logger.warning(f"Error verificando carga en intento {attempt + 1}: {e}")
                
                time.sleep(1)
            
            self.logger.warning(f"Timeout después de {timeout} segundos esperando carga completa de datos")
            return False
            
        except TimeoutException:
            self.logger.warning("Timeout esperando que aparezca la tabla principal")
            return False
            
    def extract_historico_table_data(self):
        """Extraer datos de la tabla de histórico de ventas agrupada por función"""
        try:
            historico_data = []
            
            # Buscar la tabla principal con datos agrupados
            table_selectors = [
                ".dx-datagrid-rowsview .dx-datagrid-table",
                ".dx-datagrid-rowsview table",
                ".dx-datagrid table"
            ]
            
            table = None
            for selector in table_selectors:
                try:
                    tables = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if tables:
                        table = tables[0]
                        self.logger.info(f"Tabla encontrada con selector: {selector}")
                        break
                except:
                    continue
            
            if not table:
                self.logger.warning("No se encontró la tabla de datos")
                return []
            
            # Buscar filas de datos (excluyendo headers y grupos)
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr.dx-data-row")
            self.logger.info(f"Encontradas {len(rows)} filas de datos")
            
            # Buscar el código de función en el header del grupo
            function_code = None
            function_parsed = None
            try:
                group_header = self.driver.find_element(By.CSS_SELECTOR, ".dx-group-cell")
                if group_header:
                    group_text = group_header.text.strip()
                    if "COD. FUNCIÓN:" in group_text:
                        function_code = group_text.replace("COD. FUNCIÓN:", "").strip()
                        self.logger.info(f"Código de función encontrado: {function_code}")
                        
                        # Parsear el código de función
                        function_parsed = self.parse_function_code(function_code)
            except:
                self.logger.warning("No se pudo extraer el código de función")
            
            for i, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 9:  # Según el HTML proporcionado
                        # Extraer datos de cada celda
                        fecha = cells[1].text.strip() if len(cells) > 1 else ""
                        cantidad_vendida = cells[2].text.strip() if len(cells) > 2 else ""
                        monto = cells[3].text.strip() if len(cells) > 3 else ""
                        numero_fila = cells[4].text.strip() if len(cells) > 4 else ""
                        total_acumulado = cells[5].text.strip() if len(cells) > 5 else ""
                        porcentaje = cells[6].text.strip() if len(cells) > 6 else ""
                        monto_total = cells[7].text.strip() if len(cells) > 7 else ""
                        
                        row_data = {
                            'codigo_funcion': function_code,
                            'function_parsed': function_parsed,
                            'fecha': fecha,
                            'cantidad_vendida': cantidad_vendida,
                            'monto': monto,
                            'numero_fila': numero_fila,
                            'total_acumulado': total_acumulado,
                            'porcentaje': porcentaje,
                            'monto_total': monto_total
                        }
                        
                        # Solo agregar si tiene contenido útil
                        if any(value != '' and value != '&nbsp;' for value in row_data.values() if value != 'codigo_funcion'):
                            historico_data.append(row_data)
                            self.logger.info(f"Fila {i+1}: {fecha} - {cantidad_vendida} tickets - {monto}")
                    else:
                        self.logger.debug(f"Fila {i} tiene solo {len(cells)} celdas, esperado al menos 9")
                except Exception as e:
                    self.logger.warning(f"Error procesando fila {i}: {e}")
                    
            self.logger.info(f"Extraídas {len(historico_data)} filas válidas de la tabla de histórico")
            return historico_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de tabla de histórico: {e}")
            return []
    
    def extract_main_table_data(self):
        """Extraer datos de la tabla principal (mantener para compatibilidad)"""
        return self.extract_historico_table_data()
            
    def extract_summary_table_data(self):
        """Extraer datos de la tabla de resumen (segunda tabla)"""
        try:
            summary_data = []
            
            # Buscar filas de la tabla de resumen
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportesDOS .dx-datagrid-rowsview tbody tr.dx-row")
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    row_data = {
                        'descuento': cells[0].text.strip(),
                        'ingresos': cells[1].text.strip(),
                        'total': cells[2].text.strip()
                    }
                    summary_data.append(row_data)
                    
            self.logger.info(f"Extraídas {len(summary_data)} filas de la tabla de resumen")
            return summary_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de tabla de resumen: {e}")
            return []
            
    def extract_payment_methods_data(self):
        """Extraer datos de la tabla de formas de pago"""
        try:
            payment_data = []
            
            # Buscar filas de la tabla de formas de pago
            rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportesTRES .dx-datagrid-rowsview tbody tr.dx-row")
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    row_data = {
                        'forma_pago': cells[0].text.strip(),
                        'cantidad_tickets': cells[1].text.strip(),
                        'total': cells[2].text.strip()
                    }
                    payment_data.append(row_data)
                    
            self.logger.info(f"Extraídas {len(payment_data)} filas de la tabla de formas de pago")
            return payment_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de tabla de formas de pago: {e}")
            return []
            
    def extract_totals_data(self):
        """Extraer datos de totales y resumen"""
        try:
            totals_data = {}
            
            # Extraer datos del resumen lateral
            try:
                vendidas = self.driver.find_element(By.ID, "vendidas").text.strip()
                disponibles = self.driver.find_element(By.ID, "disponibles").text.strip()
                invitaciones = self.driver.find_element(By.ID, "invitaciones").text.strip()
                capacidad = self.driver.find_element(By.ID, "capacidad").text.strip()
                bloqueadas = self.driver.find_element(By.ID, "bloqueadas").text.strip()
                kills = self.driver.find_element(By.ID, "kills").text.strip()
                devoluciones = self.driver.find_element(By.ID, "devoluciones").text.strip()
                montototal = self.driver.find_element(By.ID, "montototal").text.strip()
                totaldevoluciones = self.driver.find_element(By.ID, "totaldevoluciones").text.strip()
                totalventas = self.driver.find_element(By.ID, "totalventas").text.strip()
                
                totals_data = {
                    'vendidas': vendidas,
                    'disponibles': disponibles,
                    'invitaciones': invitaciones,
                    'capacidad': capacidad,
                    'bloqueadas': bloqueadas,
                    'kills': kills,
                    'devueltas': devoluciones,
                    'monto_total': montototal,
                    'monto_total_devuelto': totaldevoluciones,
                    'venta_total_neta': totalventas
                }
                
            except NoSuchElementException as e:
                self.logger.warning(f"Algunos elementos de totales no encontrados: {e}")
                
            self.logger.info("Datos de totales extraídos")
            return totals_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de totales: {e}")
            return {}
            
    def extract_event_info(self):
        """Extraer información del evento"""
        try:
            event_info = {}
            
            try:
                nombre_reporte = self.driver.find_element(By.ID, "nombre_reporte").text.strip()
                nombre_locacion = self.driver.find_element(By.ID, "nombre_locacion").text.strip()
                
                event_info = {
                    'nombre_evento': nombre_reporte,
                    'locacion_fecha': nombre_locacion
                }
                
            except NoSuchElementException:
                self.logger.warning("Información de evento no encontrada")
                
            return event_info
            
        except Exception as e:
            self.logger.error(f"Error extrayendo información de evento: {e}")
            return {}
            
    def extract_all_data(self, establecimiento, espectaculo, funcion):
        """Extraer todos los datos de la página actual"""
        try:
            # Pequeña pausa adicional antes de extraer
            self.logger.info("Pausa final antes de extracción...")
            time.sleep(3)
                
            # Extraer información del evento
            event_info = self.extract_event_info()
            
            # Extraer datos de todas las tablas
            main_table = self.extract_main_table_data()
            summary_table = self.extract_summary_table_data()
            payment_methods = self.extract_payment_methods_data()
            totals = self.extract_totals_data()
            
            # Compilar todos los datos
            all_data = {
                'timestamp': datetime.now().isoformat(),
                'establecimiento': establecimiento,
                'espectaculo': espectaculo,
                'funcion': funcion,
                'evento_info': event_info,
                'tabla_principal': main_table,
                'tabla_resumen': summary_table,
                'formas_pago': payment_methods,
                'totales': totals
            }
            
            return all_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo todos los datos: {e}")
            return None
            
    def save_data_to_json(self, data, filename):
        """Guardar datos en archivo JSON"""
        try:
            # Verificar que el directorio existe
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir, exist_ok=True)
                self.logger.info(f"Directorio creado: {self.output_dir}")
            
            filepath = os.path.join(self.output_dir, filename)
            self.logger.info(f"Intentando guardar en: {filepath}")
            
            # Verificar que hay datos para guardar
            if not data:
                self.logger.error("No hay datos para guardar")
                return False
                
            self.logger.info(f"Datos a guardar - Tamaño: {len(str(data))} caracteres")
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            # Verificar que el archivo se creó
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                self.logger.info(f"Archivo guardado exitosamente: {filepath} ({file_size} bytes)")
                return True
            else:
                self.logger.error(f"El archivo no se creó: {filepath}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error guardando datos en {filename}: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False
            
    def get_current_selections(self):
        """Obtener las selecciones actuales de los dropdowns"""
        try:
            selections = {}
            
            # Establecimiento
            try:
                est_input = self.driver.find_element(By.CSS_SELECTOR, "#selectEstablecimiento input[type='hidden']")
                est_value = est_input.get_attribute('value')
                est_text = self.driver.find_element(By.CSS_SELECTOR, "#selectEstablecimiento .dx-texteditor-input").get_attribute('value')
                selections['establecimiento'] = {'value': est_value, 'text': est_text}
            except:
                selections['establecimiento'] = None
                
            # Espectáculo
            try:
                esp_input = self.driver.find_element(By.CSS_SELECTOR, "#selectEspectaculo input[type='hidden']")
                esp_value = esp_input.get_attribute('value')
                esp_text = self.driver.find_element(By.CSS_SELECTOR, "#selectEspectaculo .dx-texteditor-input").get_attribute('value')
                selections['espectaculo'] = {'value': esp_value, 'text': esp_text}
            except:
                selections['espectaculo'] = None
                
            # Función
            try:
                fun_input = self.driver.find_element(By.CSS_SELECTOR, "#selectFuncion input[type='hidden']")
                fun_value = fun_input.get_attribute('value')
                fun_text = self.driver.find_element(By.CSS_SELECTOR, "#selectFuncion .dx-texteditor-input").get_attribute('value')
                selections['funcion'] = {'value': fun_value, 'text': fun_text}
            except:
                selections['funcion'] = None
                
            return selections
            
        except Exception as e:
            self.logger.error(f"Error obteniendo selecciones actuales: {e}")
            return {}
            
    def check_if_data_available(self):
        """Verificar si hay datos disponibles en las tablas"""
        try:
            # Verificar si hay datos en la tabla principal
            main_table_rows = self.driver.find_elements(By.CSS_SELECTOR, "#gridReportes .dx-datagrid-rowsview tbody tr")
            if len(main_table_rows) > 0:
                return True
            return False
        except:
            return False
    
    def run_complete_scraping(self):
        """Ejecutar el scraping completo"""
        try:
            self.setup_driver()
            
            # Login
            if not self.login():
                self.logger.error("Login falló, terminando")
                return False
                
            # Navegar a histórico de ventas
            if not self.navigate_to_historico_ventas():
                self.logger.error("Navegación a histórico de ventas falló, terminando")
                return False
                
            # Debug de la estructura de la página
            self.debug_page_structure()
            
            # Verificar las selecciones actuales
            current_selections = self.get_current_selections()
            self.logger.info(f"Selecciones actuales: {current_selections}")
            
            # No extraer datos iniciales, solo cuando se apliquen filtros
            self.logger.info("Saltando extracción inicial, solo extraerá con filtros aplicados")
                
            # Usar valores conocidos basados en el HTML proporcionado
            # Establecimiento: "Autódromo de Rosario" 
            # Espectáculo: "Alejandro Sanz"
            # NO seleccionamos función - se extrae de la tabla después de buscar
            
            establecimientos_conocidos = [
                {'text': 'Autódromo de Rosario', 'value': '145'}  # Valor estimado
            ]
            
            espectaculos_conocidos = {
                'Autódromo de Rosario': [
                    {'text': 'Alejandro Sanz', 'value': '15886'}  # Valor estimado
                ]
            }
            
            # Intentar obtener opciones dinámicamente primero
            establecimientos = self.get_dropdown_options("selectEstablecimiento")
            if not establecimientos:
                self.logger.info("Usando establecimientos conocidos")
                establecimientos = establecimientos_conocidos
                
            total_extracciones = 0
            
            for establecimiento in establecimientos:
                self.logger.info(f"Procesando establecimiento: {establecimiento['text']}")
                
                # Seleccionar establecimiento usando método forzado si es necesario
                establecimiento_seleccionado = False
                
                # Intentar selección normal primero
                if 'value' in establecimiento:
                    establecimiento_seleccionado = self.force_dropdown_selection("selectEstablecimiento", establecimiento['value'])
                
                if not establecimiento_seleccionado:
                    establecimiento_seleccionado = self.select_dropdown_option("selectEstablecimiento", establecimiento['text'])
                
                if not establecimiento_seleccionado:
                    self.logger.warning(f"No se pudo seleccionar establecimiento {establecimiento['text']}, continuando...")
                    continue
                    
                time.sleep(3)  # Esperar a que se carguen los espectáculos
                
                # Obtener espectáculos - usar conocidos si es necesario
                espectaculos = self.get_dropdown_options("selectEspectaculo")
                if not espectaculos and establecimiento['text'] in espectaculos_conocidos:
                    self.logger.info("Usando espectáculos conocidos")
                    espectaculos = espectaculos_conocidos[establecimiento['text']]
                
                for espectaculo in espectaculos:
                    self.logger.info(f"Procesando espectáculo: {espectaculo['text']}")
                    
                    # Seleccionar espectáculo usando método forzado si es necesario
                    espectaculo_seleccionado = False
                    
                    if 'value' in espectaculo:
                        espectaculo_seleccionado = self.force_dropdown_selection("selectEspectaculo", espectaculo['value'])
                    
                    if not espectaculo_seleccionado:
                        espectaculo_seleccionado = self.select_dropdown_option("selectEspectaculo", espectaculo['text'])
                    
                    if not espectaculo_seleccionado:
                        self.logger.warning(f"No se pudo seleccionar espectáculo {espectaculo['text']}, continuando...")
                        continue
                        
                    time.sleep(3)  # Esperar a que se carguen las opciones
                    
                    # Hacer clic en el botón "Buscar" para obtener resultados
                    self.logger.info("Haciendo clic en botón 'Buscar'...")
                    if not self.click_buscar_button():
                        self.logger.warning("No se pudo hacer clic en 'Buscar', continuando...")
                        continue
                    
                    # Esperar a que carguen los resultados
                    self.logger.info("Esperando a que carguen los resultados...")
                    time.sleep(10)
                    
                    # Verificar que apareció la tabla con datos
                    self.logger.info("Verificando que existan elementos de tabla...")
                    table_exists = len(self.driver.find_elements(By.CSS_SELECTOR, ".dx-datagrid-rowsview")) > 0
                    if table_exists:
                        self.logger.info("Tabla de resultados encontrada, procediendo con extracción")
                    else:
                        self.logger.warning("Tabla de resultados no encontrada, esperando 5 segundos más...")
                        time.sleep(5)
                    
                    # Extraer todos los datos
                    self.logger.info(f"Iniciando extracción de datos para: {espectaculo['text']}")
                    data = self.extract_all_data(
                        establecimiento['text'],
                        espectaculo['text'],
                        "Múltiples funciones"  # Ya no tenemos función específica
                    )
                    
                    if data:
                        # Mostrar resumen de datos extraídos
                        main_rows = len(data.get('tabla_principal', []))
                        summary_rows = len(data.get('tabla_resumen', []))
                        payment_rows = len(data.get('formas_pago', []))
                        
                        self.logger.info(f"Datos extraídos - Principal: {main_rows} filas, Resumen: {summary_rows} filas, Pagos: {payment_rows} filas")
                        
                        # Crear nombre de archivo
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"entradauno_{establecimiento['text'].replace(' ', '_')}_{espectaculo['text'].replace(' ', '_')}_{timestamp}.json"
                        
                        # Guardar datos
                        if self.save_data_to_json(data, filename):
                            total_extracciones += 1
                    else:
                        self.logger.warning(f"No se pudieron extraer datos para: {espectaculo['text']}")
                                
            self.logger.info(f"Scraping completo. Total de extracciones: {total_extracciones}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error en scraping completo: {e}")
            return False
            
        finally:
            if self.driver:
                self.driver.quit()
    
    def calculate_event_totals(self, historico_data):
        """Calcula los totales del evento desde los datos extraídos de EntradaUno"""
        try:
            # Calcular totales desde formas de pago (más confiable)
            total_vendido = 0
            total_recaudacion = 0
            formas_pago = historico_data.get('formas_pago', [])
            
            self.logger.info(f"DEBUG: Procesando {len(formas_pago)} formas de pago")
            self.logger.info(f"DEBUG: Estructura completa de datos: {list(historico_data.keys())}")
            self.logger.info(f"DEBUG: Totales disponibles: {historico_data.get('totales', {})}")
            
            for i, forma_pago in enumerate(formas_pago):
                self.logger.info(f"DEBUG: Forma pago {i}: {forma_pago}")
                
                if forma_pago.get('total') and forma_pago.get('total') != '':
                    monto = self.parse_number_with_dots(forma_pago['total'])
                    total_recaudacion += monto
                    self.logger.info(f"DEBUG: Agregado monto {monto} (total: {total_recaudacion})")
                
                if forma_pago.get('cantidad_tickets') and forma_pago.get('cantidad_tickets') != '':
                    tickets = self.parse_number_with_dots(forma_pago['cantidad_tickets'])
                    total_vendido += tickets
                    self.logger.info(f"DEBUG: Agregado {tickets} tickets (total: {total_vendido})")
            
            # Verificar si hay totales directos disponibles
            if total_vendido == 0 or total_recaudacion == 0:
                totales_directos = historico_data.get('totales', {})
                self.logger.info(f"DEBUG: Verificando totales directos: {totales_directos}")
                
                if totales_directos.get('vendidas') and totales_directos.get('vendidas') != '0':
                    total_vendido = self.parse_number_with_dots(totales_directos['vendidas'])
                    self.logger.info(f"DEBUG: Usando total vendido directo: {total_vendido}")
                
                if totales_directos.get('monto_total') and totales_directos.get('monto_total') != '$0':
                    total_recaudacion = self.parse_number_with_dots(totales_directos['monto_total'])
                    self.logger.info(f"DEBUG: Usando recaudación directa: {total_recaudacion}")
            
            # Calcular totales desde tabla_resumen como respaldo
            if total_vendido == 0 or total_recaudacion == 0:
                tabla_resumen = historico_data.get('tabla_resumen', [])
                self.logger.info(f"DEBUG: Procesando {len(tabla_resumen)} filas de tabla resumen")
                
                for fila in tabla_resumen:
                    if fila.get('ingresos') and fila.get('ingresos') != '' and not fila.get('ingresos').startswith('#'):
                        tickets = self.parse_number_with_dots(fila['ingresos'])
                        total_vendido += tickets
                    
                    if fila.get('total') and fila.get('total') != '' and not fila.get('total').startswith('$'):
                        monto = self.parse_number_with_dots(fila['total'])
                        total_recaudacion += monto
            
            # Estimar capacidad total (asumiendo que es mayor que vendido)
            total_capacidad = max(total_vendido, 1000)  # Mínimo 1000 como estimación
            total_disponible = max(0, total_capacidad - total_vendido)
            
            # Calcular porcentaje de ocupación
            porcentaje_ocupacion = 0
            if total_capacidad > 0:
                porcentaje_ocupacion = (total_vendido / total_capacidad) * 100
            
            totales = {
                'capacidad_total': total_capacidad,
                'vendido_total': total_vendido,
                'disponible_total': total_disponible,
                'recaudacion_total_ars': total_recaudacion,
                'porcentaje_ocupacion': round(porcentaje_ocupacion, 2)
            }
            
            self.logger.info(f"📊 Totales calculados para evento:")
            self.logger.info(f"  📊 Capacidad: {total_capacidad}")
            self.logger.info(f"  🎫 Vendido: {total_vendido}")
            self.logger.info(f"  🆓 Disponible: {total_disponible}")
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
        """Parsea la fecha del evento desde el formato de EntradaUno"""
        try:
            self.logger.info(f"🔍 DEBUG: Parseando fecha: '{fecha_str}'")
            
            # Formato típico de EntradaUno: "DD/MM/YYYY" o "DD/MM/YY"
            if re.match(r'\\d{1,2}/\\d{1,2}/\\d{2,4}', fecha_str):
                try:
                    # Extraer las partes de la fecha
                    parts = fecha_str.split('/')
                    day = int(parts[0])
                    month = int(parts[1])
                    year = int(parts[2])
                    
                    # Manejar años de 2 dígitos
                    if year < 100:
                        year += 2000
                    
                    fecha_parsed = f"{year}-{month:02d}-{day:02d}"
                    self.logger.info(f"🔍 DEBUG: Fecha parseada: '{fecha_parsed}'")
                    return fecha_parsed
                    
                except Exception as e:
                    self.logger.warning(f"Error parseando formato EntradaUno: {e}")
            
            # Si no se puede parsear, usar fecha actual
            fecha_default = datetime.now().strftime("%Y-%m-%d")
            self.logger.warning(f"⚠️ No se pudo parsear la fecha '{fecha_str}', usando fecha actual: {fecha_default}")
            return fecha_default
            
        except Exception as e:
            self.logger.error(f"Error parseando fecha: {e}")
            fecha_default = datetime.now().strftime("%Y-%m-%d")
            self.logger.warning(f"⚠️ Error parseando fecha, usando fecha actual: {fecha_default}")
            return fecha_default
    
    def save_single_event_to_database(self, historico_data, evento_info):
        """Guarda los datos de un evento individual en la base de datos"""
        try:
            if not self.db_connection:
                self.logger.error("No hay conexión a la base de datos")
                return False
            
            # Calcular totales
            totales = self.calculate_event_totals(historico_data)
            
            # Parsear fecha del evento
            fecha_show = self.parse_fecha_evento(evento_info.get('fecha', ''))
            
            # Extraer artista y venue del nombre del evento
            evento_nombre = evento_info.get('nombre', 'Evento EntradaUno')
            # Intentar extraer artista y venue del nombre
            if ' - ' in evento_nombre:
                artista, venue = evento_nombre.split(' - ', 1)
            else:
                artista = evento_nombre
                venue = "EntradaUno Venue"
            
            # Estructurar datos para la base de datos
            json_individual = {
                'ticketera': 'entradauno',
                'artista': artista.strip(),
                'venue': venue.strip(),
                'fecha_show': fecha_show,
                'evento_nombre': evento_nombre,
                'ciudad': venue.strip(),  # Usar venue como ciudad por defecto
                'historico_data': historico_data,
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
                'entradauno',
                json_individual['artista'],
                json_individual['venue'],
                fecha_show,
                json.dumps(json_individual, ensure_ascii=False),
                fecha_extraccion_argentina,
                f"entradauno_historico_{evento_nombre.replace(' ', '_')}",
                self.target_url,
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
    
    def save_data_to_database(self, all_historico_data):
        """Save all extracted data to database"""
        try:
            if not all_historico_data:
                self.logger.warning("No hay datos para guardar")
                return

            self.logger.info(f"💾 Datos ya guardados durante la extracción individual de eventos")
            self.logger.info(f"✅ Total de eventos procesados: {len(all_historico_data)}")

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
            
            # Realizar login
            if not self.login():
                self.logger.error("Login fallido")
                return False
            
            # Navegar a histórico de ventas
            if not self.navigate_to_historico_ventas():
                self.logger.error("Navegación a histórico de ventas falló, terminando")
                return False
            
            # Usar valores conocidos basados en el HTML proporcionado
            establecimientos_conocidos = [
                {'text': 'Autódromo de Rosario', 'value': '145'}
            ]
            
            espectaculos_conocidos = {
                'Autódromo de Rosario': [
                    {'text': 'Alejandro Sanz', 'value': '15886'}
                ]
            }
            
            # Intentar obtener opciones dinámicamente primero
            establecimientos = self.get_dropdown_options("selectEstablecimiento")
            if not establecimientos:
                self.logger.info("Usando establecimientos conocidos")
                establecimientos = establecimientos_conocidos
                
            all_historico_data = []
            
            for establecimiento in establecimientos:
                self.logger.info(f"Procesando establecimiento: {establecimiento['text']}")
                
                # Seleccionar establecimiento
                if 'value' in establecimiento:
                    establecimiento_seleccionado = self.force_dropdown_selection("selectEstablecimiento", establecimiento['value'])
                else:
                    establecimiento_seleccionado = self.select_dropdown_option("selectEstablecimiento", establecimiento['text'])
                
                if not establecimiento_seleccionado:
                    self.logger.warning(f"No se pudo seleccionar establecimiento {establecimiento['text']}, continuando...")
                    continue
                    
                time.sleep(3)
                
                # Obtener espectáculos
                espectaculos = self.get_dropdown_options("selectEspectaculo")
                if not espectaculos and establecimiento['text'] in espectaculos_conocidos:
                    self.logger.info("Usando espectáculos conocidos")
                    espectaculos = espectaculos_conocidos[establecimiento['text']]
                
                for espectaculo in espectaculos:
                    self.logger.info(f"Procesando espectáculo: {espectaculo['text']}")
                    
                    # Seleccionar espectáculo
                    if 'value' in espectaculo:
                        espectaculo_seleccionado = self.force_dropdown_selection("selectEspectaculo", espectaculo['value'])
                    else:
                        espectaculo_seleccionado = self.select_dropdown_option("selectEspectaculo", espectaculo['text'])
                    
                    if not espectaculo_seleccionado:
                        self.logger.warning(f"No se pudo seleccionar espectáculo {espectaculo['text']}, continuando...")
                        continue
                        
                    time.sleep(3)
                    
                    # Hacer clic en "Buscar" para obtener resultados
                    self.logger.info("Haciendo clic en botón 'Buscar'...")
                    if not self.click_buscar_button():
                        self.logger.warning("No se pudo hacer clic en 'Buscar', continuando...")
                        continue
                    
                    # Esperar a que carguen los resultados
                    self.logger.info("Esperando a que carguen los resultados...")
                    time.sleep(10)
                    
                    # Extraer datos
                    data = self.extract_all_data(
                        establecimiento['text'],
                        espectaculo['text'],
                        "Múltiples funciones"
                    )
                    
                    if data:
                        # Crear estructura de datos para la base de datos
                        evento_info = {
                            'nombre': f"{espectaculo['text']} - {establecimiento['text']}",
                            'fecha': "Múltiples funciones"
                        }
                        
                        all_historico_data.append(data)
                        # Guardar en base de datos inmediatamente
                        self.save_single_event_to_database(data, evento_info)
                    else:
                        self.logger.error(f"No se pudieron extraer datos para {espectaculo['text']}")
            
            # Guardar datos finales
            self.save_data_to_database(all_historico_data)
            
            self.logger.info("=== SCRAPER EJECUTADO EXITOSAMENTE ===")
            return True
            
        except Exception as e:
            self.logger.error(f"Error en el proceso principal: {e}")
            return False
        finally:
            self.close()
                
def test_scraper():
    """Función para probar el scraper en modo test"""
    print("🧪 INICIANDO MODO TEST DEL SCRAPER ENTRADAUNO")
    print("=" * 50)
    
    scraper = EntradaUnoScraper(headless=False, test_mode=True)  # No headless para ver el proceso
    
    try:
        # Configurar driver
        if not scraper.setup_driver():
            print("❌ No se pudo configurar el driver")
            return False
        
        # Realizar login
        if not scraper.login():
            print("❌ Login fallido")
            return False
        
        # Navegar a histórico de ventas
        if not scraper.navigate_to_historico_ventas():
            print("❌ Navegación a histórico de ventas falló")
            return False
        
        # Debug de la estructura de la página
        scraper.debug_page_structure()
        
        # Probar extracción de datos con valores conocidos
        print("\n🔍 PROBANDO EXTRACCIÓN DE DATOS...")
        
        # Usar valores conocidos
        establecimiento_text = 'Autódromo de Rosario'
        espectaculo_text = 'Alejandro Sanz'
        
        # Seleccionar establecimiento
        if scraper.force_dropdown_selection("selectEstablecimiento", establecimiento_text):
            print(f"✅ Establecimiento seleccionado: {establecimiento_text}")
            time.sleep(8)  # Esperar más tiempo para que carguen los espectáculos
            
            # Verificar si el dropdown de espectáculos está habilitado
            try:
                espectaculo_input = scraper.driver.find_element(By.CSS_SELECTOR, "#selectEspectaculo .dx-texteditor-input")
                is_enabled = espectaculo_input.is_enabled()
                print(f"🔍 Dropdown de espectáculos habilitado: {is_enabled}")
                
                if not is_enabled:
                    print("⚠️ Dropdown de espectáculos aún deshabilitado, esperando más tiempo...")
                    time.sleep(5)
                    
                    # Intentar habilitar con JavaScript
                    script = """
                    var dropdown = $("#selectEspectaculo").dxSelectBox("instance");
                    if (dropdown) {
                        dropdown.option("disabled", false);
                        return true;
                    }
                    return false;
                    """
                    result = scraper.driver.execute_script(script)
                    print(f"🔧 JavaScript para habilitar espectáculos: {result}")
                    time.sleep(3)
                    
            except Exception as e:
                print(f"⚠️ Error verificando estado del dropdown: {e}")
            
            # Seleccionar espectáculo
            if scraper.force_dropdown_selection("selectEspectaculo", espectaculo_text):
                print(f"✅ Espectáculo seleccionado: {espectaculo_text}")
                time.sleep(3)
                
                # Hacer clic en "Buscar"
                print("🔍 Haciendo clic en botón 'Buscar'...")
                if scraper.click_buscar_button():
                    print("✅ Botón 'Buscar' clickeado exitosamente")
                    time.sleep(10)
                    
                    # Extraer datos
                    print("\n📊 EXTRAYENDO DATOS...")
                    data = scraper.extract_all_data(
                        establecimiento_text,
                        espectaculo_text,
                        "Múltiples funciones"
                    )
                    
                    if data:
                        print("\n✅ DATOS EXTRAÍDOS EXITOSAMENTE:")
                        print(f"📅 Timestamp: {data['timestamp']}")
                        print(f"🏢 Establecimiento: {data['establecimiento']}")
                        print(f"🎭 Espectáculo: {data['espectaculo']}")
                        print(f"📅 Función: {data['funcion']}")
                        
                        # Mostrar datos específicos de histórico
                        if data.get('tabla_principal'):
                            print(f"\n📋 DATOS DE HISTÓRICO:")
                            for i, row in enumerate(data['tabla_principal'][:3]):  # Mostrar primeras 3 filas
                                print(f"  Fila {i+1}: {row.get('fecha', 'N/A')} - {row.get('cantidad_vendida', 'N/A')} tickets - {row.get('monto', 'N/A')}")
                                if row.get('codigo_funcion'):
                                    print(f"    Código función: {row['codigo_funcion']}")
                        
                        # Mostrar resumen de datos
                        main_rows = len(data.get('tabla_principal', []))
                        summary_rows = len(data.get('tabla_resumen', []))
                        payment_rows = len(data.get('formas_pago', []))
                        
                        print(f"\n📊 RESUMEN DE DATOS:")
                        print(f"  📋 Tabla principal: {main_rows} filas")
                        print(f"  📋 Tabla resumen: {summary_rows} filas")
                        print(f"  💳 Formas de pago: {payment_rows} filas")
                        
                        # Mostrar totales
                        totales = data.get('totales', {})
                        if totales:
                            print(f"\n💰 TOTALES:")
                            for key, value in totales.items():
                                print(f"  {key}: {value}")
                        
                        # Guardar datos de test
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"test_entradauno_data_{timestamp}.json"
                        if scraper.save_data_to_json(data, filename):
                            print(f"\n💾 Datos guardados en: {filename}")
                        
                        return True
                    else:
                        print("❌ No se pudieron extraer datos")
                        return False
                else:
                    print("❌ No se pudo hacer clic en 'Buscar'")
                    return False
            else:
                print("❌ No se pudo seleccionar espectáculo")
                return False
        else:
            print("❌ No se pudo seleccionar establecimiento")
            return False
             
    except Exception as e:
        print(f"❌ Error en modo test: {e}")
        return False
    finally:
        scraper.close()

def main():
    """Función principal para ejecutar el scraper"""
    scraper = EntradaUnoScraper(headless=True)
    success = scraper.run()
    
    if success:
        print("✅ Scraper ejecutado exitosamente")
    else:
        print("❌ Error ejecutando el scraper")
    
    return success

def test_all_establecimientos_espectaculos():
    """Función para probar el scraper con todos los establecimientos y espectáculos"""
    print("🧪 INICIANDO TEST COMPLETO - TODOS LOS ESTABLECIMIENTOS Y ESPECTÁCULOS")
    print("=" * 70)
    
    try:
        # Crear instancia del scraper en modo test
        scraper = EntradaUnoScraper(headless=False, test_mode=True)
        
        # Configurar driver
        if not scraper.setup_driver():
            print("❌ Error configurando el driver")
            return False
        
        # Hacer login
        if not scraper.login():
            print("❌ Error en el login")
            return False
        
        # Navegar a la página de histórico de ventas
        if not scraper.navigate_to_historico_ventas():
            print("❌ Error navegando a histórico de ventas")
            return False
        
        # Obtener todos los establecimientos
        print("\n🔍 OBTENIENDO TODOS LOS ESTABLECIMIENTOS...")
        establecimientos = scraper.get_all_establecimientos()
        
        if not establecimientos:
            print("❌ No se encontraron establecimientos")
            return False
        
        all_data = []
        total_processed = 0
        
        # Procesar cada establecimiento
        for i, establecimiento in enumerate(establecimientos):
            print(f"\n🏢 PROCESANDO ESTABLECIMIENTO {i+1}/{len(establecimientos)}: {establecimiento['text']}")
            
            # Recargar la página para resetear los dropdowns (excepto en el primer establecimiento)
            if i > 0:
                print("🔄 Recargando página para resetear dropdowns...")
                scraper.driver.refresh()
                time.sleep(5)
                print("✅ Página recargada")
            
            # Obtener espectáculos para este establecimiento
            espectaculos = scraper.get_all_espectaculos(establecimiento['text'])
            
            if not espectaculos:
                print(f"⚠️ No se encontraron espectáculos para {establecimiento['text']}")
                continue
            
            # Procesar cada espectáculo
            for j, espectaculo in enumerate(espectaculos):
                print(f"\n🎭 PROCESANDO ESPECTÁCULO {j+1}/{len(espectaculos)}: {espectaculo['text']}")
                
                try:
                    # Seleccionar espectáculo
                    if scraper.force_dropdown_selection("selectEspectaculo", espectaculo['text']):
                        print(f"✅ Espectáculo seleccionado: {espectaculo['text']}")
                        time.sleep(3)
                        
                        # Hacer clic en "Buscar"
                        print("🔍 Haciendo clic en botón 'Buscar'...")
                        if scraper.click_buscar_button():
                            print("✅ Botón 'Buscar' clickeado exitosamente")
                            time.sleep(10)
                            
                            # Extraer datos
                            print("📊 EXTRAYENDO DATOS...")
                            data = scraper.extract_all_data(
                                establecimiento['text'],
                                espectaculo['text'],
                                "Múltiples funciones"
                            )
                            
                            if data and data.get('tabla_principal'):
                                print(f"✅ Datos extraídos: {len(data['tabla_principal'])} filas")
                                
                                # Mostrar información parseada del código de función
                                if data['tabla_principal']:
                                    first_row = data['tabla_principal'][0]
                                    if first_row.get('function_parsed'):
                                        parsed = first_row['function_parsed']
                                        print(f"  🎵 Artista: {parsed.get('artista_nombre', 'N/A')}")
                                        print(f"  📅 Fecha: {parsed.get('fecha_formateada', 'N/A')}")
                                        print(f"  🕐 Hora: {parsed.get('hora_formateada', 'N/A')}")
                                
                                all_data.append(data)
                                total_processed += 1
                            else:
                                print("⚠️ No se encontraron datos para este espectáculo")
                        else:
                            print("❌ No se pudo hacer clic en 'Buscar'")
                    else:
                        print("❌ No se pudo seleccionar espectáculo")
                        
                except Exception as e:
                    print(f"❌ Error procesando {espectaculo['text']}: {e}")
                    continue
                
                # Recargar página después de cada espectáculo para limpiar los dropdowns
                if j < len(espectaculos) - 1:  # No recargar en el último espectáculo
                    print("🔄 Recargando página para limpiar dropdowns...")
                    scraper.driver.refresh()
                    time.sleep(5)
                    print("✅ Página recargada")
        
        # Guardar todos los datos
        if all_data:
            import json
            import os
            from datetime import datetime
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"test_all_entradauno_data_{timestamp}.json"
            filepath = os.path.join(scraper.output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            
            print(f"\n🎉 PROCESAMIENTO COMPLETADO:")
            print(f"  📊 Total de combinaciones procesadas: {total_processed}")
            print(f"  💾 Datos guardados en: {filename}")
            return True
        else:
            print("❌ No se extrajeron datos de ninguna combinación")
            return False
            
    except Exception as e:
        print(f"❌ Error en el test completo: {e}")
        return False
    finally:
        if 'scraper' in locals():
            scraper.close()

if __name__ == "__main__":
    import sys
    
    # Verificar si se quiere ejecutar en modo test
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_scraper()
        elif sys.argv[1] == "test_all":
            test_all_establecimientos_espectaculos()
        else:
            print("Uso: python entradauno_scraper.py [test|test_all]")
    else:
        main()

def run_scraper_for_airflow():
    """Función para ejecutar el scraper desde Airflow"""
    scraper = EntradaUnoScraper(headless=True)
    return scraper.run()
