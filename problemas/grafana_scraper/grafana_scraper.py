#!/usr/bin/env python3
"""
Scraper para extraer datos de dashboards de Grafana
Automatiza el login SSO y extrae información de múltiples dashboards
"""

import time
import random
import csv
import json
import logging
from datetime import datetime

# --- Selenium & anti-detección ---
# Intentamos usar undetected_chromedriver (UC) que ofrece múltiples
# contra-medidas contra la detección de automatización.
# Si UC no está disponible (por ej. en entorno sin pip install),
# hacemos fallback al webdriver estándar para no romper la ejecución.

try:
    import undetected_chromedriver as uc  # type: ignore
    _UC_AVAILABLE = True
except ImportError:
    _UC_AVAILABLE = False

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import os

class GrafanaScraper:
    def __init__(self):
        self.base_url = "https://g-cc18438941.grafana-workspace.us-east-2.amazonaws.com"
        self.login_url = f"{self.base_url}/login"
        self.dashboards_url = f"{self.base_url}/dashboards"
        
        # Pool de User-Agents realistas para rotación
        self.user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
        ]
        self.current_ua_index = 0
        
        # Configurar logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('grafana_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Crear directorio para resultados
        self.output_dir = "jsongrafana"
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.driver = None
        self.dashboard_urls = []
        self.session_count = 0
        
    def setup_driver(self):
        """Configura el driver de Chrome con opciones avanzadas anti-detección"""
        chrome_options = Options()
        
        # === CONFIGURACIONES ULTRA STEALTH ===
        
        # Básicas anti-detección
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        
        # Configuraciones que solo se aplican si NO usamos UC (para evitar conflictos)
        self.use_uc_options = True  # Flag para determinar qué opciones usar
        
        # Anti-detección avanzadas
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--no-default-browser-check')
        chrome_options.add_argument('--disable-plugins-discovery')
        chrome_options.add_argument('--disable-preconnect')
        chrome_options.add_argument('--disable-background-networking')
        
        # === NUEVAS BANDERAS ULTRA STEALTH ===
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-renderer-backgrounding')
        chrome_options.add_argument('--disable-backgrounding-occluded-windows')
        chrome_options.add_argument('--disable-client-side-phishing-detection')
        chrome_options.add_argument('--disable-sync')
        chrome_options.add_argument('--disable-default-apps')
        chrome_options.add_argument('--disable-component-extensions-with-background-pages')
        chrome_options.add_argument('--disable-ipc-flooding-protection')
        chrome_options.add_argument('--disable-hang-monitor')
        chrome_options.add_argument('--disable-prompt-on-repost')
        chrome_options.add_argument('--disable-domain-reliability')
        chrome_options.add_argument('--disable-component-update')
        chrome_options.add_argument('--disable-background-downloads')
        
        # User agent rotativo para evadir detección
        current_ua = self.user_agents[self.current_ua_index % len(self.user_agents)]
        chrome_options.add_argument(f'--user-agent={current_ua}')
        self.logger.info(f"   🔄 Usando User-Agent #{self.current_ua_index + 1}: {current_ua[:50]}...")
        
        # Tamaño de ventana más común y realista
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        
        # === PERFIL ROTATIVO PARA EVADIR DETECCIÓN ===
        # Cambiar perfil en cada sesión para parecer diferentes usuarios
        import tempfile
        profile_path = os.path.join(tempfile.gettempdir(), f"grafana_scraper_profile_{self.session_count}")
        chrome_options.add_argument(f'--user-data-dir={profile_path}')
        chrome_options.add_argument('--profile-directory=Default')
        self.logger.info(f"   📁 Usando perfil de sesión #{self.session_count}")
        
        # Incrementar contador de sesión para próximo uso
        self.session_count += 1
        
        # Preferencias ultra realistas
        prefs = {
            "profile.default_content_setting_values": {
                "notifications": 2,
                "geolocation": 2,
                "media_stream": 2,
                "media_stream_mic": 2,
                "media_stream_camera": 2
            },
            "profile.managed_default_content_settings": {
                "images": 1
            },
            "profile.default_content_settings": {
                "popups": 0
            },
            "profile.password_manager_enabled": False,
            "credentials_enable_service": False,
            # Configuraciones de idioma más realistas
            "intl.accept_languages": "en-US,en,es",
            "profile.managed_default_content_settings": {
                "images": 1
            }
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # --------------------
        # Inicializar el driver con la mejor opción disponible (UC > webdriver)
        # --------------------

        if _UC_AVAILABLE:
            try:
                # Para UC, usamos opciones mínimas para evitar conflictos
                uc_options = Options()
                uc_options.add_argument('--no-sandbox')
                uc_options.add_argument('--disable-dev-shm-usage')
                uc_options.add_argument('--disable-gpu')
                uc_options.add_argument('--disable-web-security')
                uc_options.add_argument('--disable-features=VizDisplayCompositor')
                uc_options.add_argument('--disable-extensions')
                uc_options.add_argument('--no-default-browser-check')
                uc_options.add_argument('--disable-plugins-discovery')
                uc_options.add_argument('--disable-preconnect')
                uc_options.add_argument('--disable-background-networking')
                uc_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36')
                uc_options.add_argument('--window-size=1366,768')
                
                # Preferencias básicas
                prefs = {
                    "profile.default_content_setting_values": {"notifications": 2},
                    "profile.managed_default_content_settings": {"images": 1}
                }
                uc_options.add_experimental_option("prefs", prefs)
                
                self.driver = uc.Chrome(options=uc_options, headless=False, use_subprocess=True)
                self.logger.info("⚡ undetected_chromedriver inicializado correctamente")
                self.use_uc_options = True
            except Exception as uc_err:
                self.logger.warning(f"Falló la creación de undetected_chromedriver ({uc_err}), se usará webdriver estándar")
                self.use_uc_options = False
                # Para webdriver estándar, añadimos las opciones anti-detección
                chrome_options.add_argument('--disable-blink-features=AutomationControlled')
                chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                chrome_options.add_experimental_option('useAutomationExtension', False)
                self.driver = webdriver.Chrome(options=chrome_options)
        else:
            self.logger.warning("undetected_chromedriver no disponible – utilizando webdriver estándar (podría ser más detectable)")
            self.use_uc_options = False
            # Para webdriver estándar, añadimos las opciones anti-detección
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            self.driver = webdriver.Chrome(options=chrome_options)
        
        # Stealth extra – solo aplicamos scripts adicionales si NO usamos UC
        # ya que UC maneja la mayoría de estos automáticamente
        if not self.use_uc_options:
            stealth_scripts = [
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})",
                "Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})",
                "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'es']})",
                "window.chrome = { runtime: {} }",
                "Object.defineProperty(navigator, 'permissions', {get: () => ({ query: () => Promise.resolve({ state: 'granted' }) })})",
                "delete navigator.__proto__.webdriver",
                # Simular parámetros WebGL comunes (vendor/product) – evita fingerprints vacíos
                "const getParameter = WebGLRenderingContext.prototype.getParameter; WebGLRenderingContext.prototype.getParameter = function(parameter){ if(parameter === 37445){ return 'Intel Inc.'; } if(parameter === 37446){ return 'Intel Iris OpenGL Engine'; } return getParameter(parameter); };"
            ]
            
            for script in stealth_scripts:
                try:
                    self.driver.execute_script(script)
                except:
                    pass
        
        # Simular comportamiento humano inicial más realista
        self.simulate_human_browsing()
        
        self.logger.info("Driver de Chrome configurado con anti-detección avanzada")
        
    def simulate_human_browsing(self):
        """Simula navegación humana ultra realista para establecer patrones normales"""
        try:
            self.logger.info("🌐 Iniciando simulación de navegación humana EXTENDIDA...")
            self.logger.info("   📊 Estableciendo historial de navegación convincente para AWS...")
            
            # === FASE 1: Comportamiento de startup natural ===
            # Visitar Google como primera página (comportamiento típico)
            self.driver.get("https://www.google.com")
            self.human_delay(4, 8)  # Tiempo más realista de carga
            
            # Simular lectura de la página (scroll lento hacia abajo)
            self.driver.execute_script("window.scrollBy(0, 150);")
            self.human_delay(1, 2)
            
            # Movimientos de mouse muy naturales y graduales
            actions = ActionChains(self.driver)
            
            # Simular movimiento hacia la barra de búsqueda
            try:
                search_box = self.driver.find_element(By.NAME, "q")
                actions.move_to_element(search_box)
                actions.pause(random.uniform(0.8, 1.5))
                actions.perform()
                self.human_delay(1, 2)
                
                # Simular escribir algo y borrarlo (comportamiento humano común)
                search_box.send_keys("grafana")
                self.human_delay(1, 2)
                search_box.clear()
                self.human_delay(0.5, 1)
                
            except:
                # Si no encuentra la barra de búsqueda, hacer movimientos aleatorios
                for _ in range(4):
                    x = random.randint(200, 800)
                    y = random.randint(150, 400)
                    actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), x, y)
                    actions.pause(random.uniform(0.8, 2.0))
                    actions.perform()
            
            # === FASE 2: Navegación EXTENDIDA a sitios relacionados ===
            # Crear un patrón de navegación que sugiera interés legítimo en AWS/Grafana
            sites_sequence = [
                "https://aws.amazon.com",
                "https://docs.aws.amazon.com/grafana/",
                "https://grafana.com/docs/"
            ]
            
            for i, site in enumerate(sites_sequence):
                self.logger.info(f"   📍 Navegando a sitio relacionado {i+1}/3: {site}")
                self.driver.get(site)
                
                # Tiempo variable de "lectura" en cada sitio
                reading_time = random.uniform(8, 15)
                self.logger.info(f"   📖 Simulando lectura ({reading_time:.1f}s)...")
                time.sleep(reading_time)
                
                # Scroll y movimientos durante la "lectura"
                for scroll_step in range(2):
                    scroll_amount = random.randint(200, 500)
                    self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                    self.human_delay(2, 4)
                    
                    # Movimientos de mouse ocasionales
                    if random.random() < 0.8:
                        actions = ActionChains(self.driver)
                        x = random.randint(200, 800)
                        y = random.randint(200, 600)
                        actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), x, y)
                        actions.pause(random.uniform(0.5, 1.5))
                        actions.perform()
                
                # Pausa entre sitios
                if i < len(sites_sequence) - 1:
                    inter_site_delay = random.uniform(3, 8)
                    self.logger.info(f"   ⏳ Pausa entre sitios ({inter_site_delay:.1f}s)...")
                    time.sleep(inter_site_delay)
            
            # === FASE 3: Comportamiento de lectura realista ===
            # Scroll gradual como si estuviéramos leyendo
            for i in range(3):
                scroll_amount = random.randint(150, 400)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                self.human_delay(2, 4)  # Tiempo de lectura
                
                # Movimientos de mouse ocasionales durante la lectura
                if random.random() < 0.7:  # 70% de probabilidad
                    actions = ActionChains(self.driver)
                    x = random.randint(100, 700)
                    y = random.randint(200, 500)
                    actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), x, y)
                    actions.pause(random.uniform(0.5, 1.2))
                    actions.perform()
            
            # === FASE 4: Simular interacción con elementos ===
            try:
                # Buscar enlaces y hacer hover sobre ellos (sin hacer click)
                links = self.driver.find_elements(By.TAG_NAME, "a")[:5]  # Solo los primeros 5
                for link in links:
                    if random.random() < 0.4:  # 40% de probabilidad de hacer hover
                        actions = ActionChains(self.driver)
                        actions.move_to_element(link)
                        actions.pause(random.uniform(0.3, 0.8))
                        actions.perform()
                        self.human_delay(0.5, 1.0)
            except:
                pass
            
            # === FASE 5: Simulación de búsqueda de información ===
            # Simular que estamos "investigando" sobre Grafana/AWS antes del login
            self.logger.info("   🔍 Simulando búsqueda de información sobre Grafana...")
            
            # Volver a Google para hacer una búsqueda relacionada
            self.driver.get("https://www.google.com")
            self.human_delay(3, 5)
            
            try:
                # Simular búsqueda de "aws grafana login"
                search_box = self.driver.find_element(By.NAME, "q")
                actions = ActionChains(self.driver)
                actions.move_to_element(search_box)
                actions.pause(random.uniform(1.0, 2.0))
                actions.perform()
                
                # Escribir búsqueda relacionada
                search_query = "aws grafana dashboard"
                for char in search_query:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.08, 0.15))
                
                self.human_delay(1, 3)
                
                # Simular que "cambiamos de opinión" y borramos
                search_box.clear()
                self.human_delay(1, 2)
                
            except:
                pass
            
            # === FASE 6: Comportamiento pre-trabajo ===
            # Scroll de vuelta arriba como preparándose para ir al sitio objetivo
            self.driver.execute_script("window.scrollTo(0, 0);")
            self.human_delay(2, 4)
            
            # Último movimiento de mouse antes de ir al sitio objetivo
            actions = ActionChains(self.driver)
            actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), 400, 200)
            actions.pause(random.uniform(2.0, 4.0))
            actions.perform()
            
            self.logger.info("   ✅ Simulación de navegación humana EXTENDIDA completada")
            self.logger.info("   📊 Historial establecido: Google → AWS → Docs → Búsqueda → Objetivo")
            
        except Exception as e:
            self.logger.warning(f"Error en simulación de navegación inicial: {e}")
            # Fallback mínimo pero funcional
            try:
                self.driver.get("https://www.google.com")
                self.human_delay(2, 4)
                # Movimiento básico de mouse
                actions = ActionChains(self.driver)
                actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), 400, 300)
                actions.pause(1.0)
                actions.perform()
                self.human_delay(1, 2)
            except:
                pass
                
    def restart_session(self):
        """Reinicia completamente la sesión del navegador para evadir detección"""
        self.logger.info("🔄 Reiniciando sesión completa para evadir detección...")
        
        # Cerrar driver actual
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
        
        # Rotar User-Agent
        self.current_ua_index += 1
        
        # Esperar un tiempo realista entre sesiones
        session_delay = random.uniform(10, 20)
        self.logger.info(f"   ⏳ Esperando {session_delay:.1f}s entre sesiones...")
        time.sleep(session_delay)
        
        # Configurar nuevo driver con nueva identidad
        self.setup_driver()
        
    def human_delay(self, min_delay=1, max_delay=3):
        """Simula delays humanos aleatorios con patrones más realistas"""
        # Usar distribución más realista (no uniforme)
        if random.random() < 0.3:  # 30% de las veces delay más largo
            delay = random.uniform(max_delay, max_delay * 1.5)
        else:
            delay = random.uniform(min_delay, max_delay)
        
        # Simular micro-pausas humanas
        if delay > 2:
            # Dividir delay largo en micro-pausas
            chunks = int(delay / 0.5)
            for _ in range(chunks):
                time.sleep(0.5 + random.uniform(-0.1, 0.1))
        else:
            time.sleep(delay)
        
    def human_scroll(self, element=None):
        """Simula scroll humano"""
        if element:
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth'});", element)
        else:
            # Scroll aleatorio en la página
            scroll_height = random.randint(200, 800)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_height});")
        self.human_delay(0.5, 1.5)
        
    def human_click(self, element):
        """Simula click humano con movimiento del mouse más realista"""
        try:
            # Mover el mouse al elemento con trayectoria más humana
            actions = ActionChains(self.driver)
            
            # Obtener posición del elemento
            location = element.location
            size = element.size
            
            # Mover a una posición aleatoria cerca del elemento primero
            offset_x = random.randint(-50, 50)
            offset_y = random.randint(-20, 20)
            actions.move_to_element_with_offset(element, offset_x, offset_y)
            actions.pause(random.uniform(0.1, 0.3))
            
            # Luego mover al elemento real
            center_x = random.randint(-size['width']//4, size['width']//4)
            center_y = random.randint(-size['height']//4, size['height']//4)
            actions.move_to_element_with_offset(element, center_x, center_y)
            actions.pause(random.uniform(0.2, 0.5))
            
            # Simular pequeño movimiento antes del click (como humanos)
            micro_x = random.randint(-2, 2)
            micro_y = random.randint(-2, 2)
            actions.move_by_offset(micro_x, micro_y)
            actions.pause(random.uniform(0.05, 0.15))
            
            # Click
            actions.click()
            actions.perform()
            
            self.human_delay(0.3, 0.8)
            return True
        except Exception as e:
            self.logger.error(f"Error en human_click: {e}")
            # Fallback a click normal
            try:
                element.click()
                self.human_delay(0.3, 0.8)
                return True
            except:
                return False
            
    def fill_credentials_human(self, username_field, password_field=None):
        """Llena las credenciales de forma ultra humana y realista"""
        try:
            # === FASE 1: PREPARACIÓN REALISTA ===
            self.logger.info("   📝 Preparando ingreso de credenciales...")
            
            # Simular que el usuario mira el campo antes de hacer click
            self.human_delay(0.5, 1.2)
            
            # Hacer click en el campo de username de forma más humana
            self.human_click(username_field)
            self.human_delay(0.3, 0.7)
            
            # Limpiar campo con comportamiento humano
            username_field.clear()
            self.human_delay(0.4, 0.8)  # Pausa realista antes de escribir
            
            # === FASE 2: ESCRITURA ULTRA REALISTA ===
            self.logger.info("   ⌨️ Escribiendo nombre de usuario...")
            username = "Daleplay1"
            
            # Simular que a veces empezamos a escribir más lento (comportamiento humano)
            initial_delay_multiplier = random.uniform(1.2, 2.0)
            
            for i, char in enumerate(username):
                # === ERRORES DE ESCRITURA REALISTAS ===
                if random.random() < 0.08 and i > 2:  # 8% de probabilidad de error
                    # Tipos de errores más realistas
                    error_types = ['wrong_char', 'double_char', 'case_error']
                    error_type = random.choice(error_types)
                    
                    if error_type == 'wrong_char':
                        # Caracter adyacente en el teclado
                        adjacent_chars = {
                            'a': 'sq', 'l': 'ko', 'e': 'wr', 'p': 'ol',
                            'y': 'tu', '1': '2q'
                        }
                        wrong_char = random.choice(adjacent_chars.get(char.lower(), 'x'))
                        username_field.send_keys(wrong_char)
                        time.sleep(random.uniform(0.15, 0.35))  # Pausa para "darse cuenta"
                        username_field.send_keys('\b')  # Corregir
                        time.sleep(random.uniform(0.08, 0.20))
                    
                    elif error_type == 'double_char' and i > 0:
                        # Escribir el caracter anterior de nuevo (error común)
                        username_field.send_keys(username[i-1])
                        time.sleep(random.uniform(0.10, 0.25))
                        username_field.send_keys('\b')
                        time.sleep(random.uniform(0.05, 0.15))
                
                # Escribir el caracter correcto
                username_field.send_keys(char)
                
                # === DELAYS ULTRA REALISTAS ===
                base_delay = 0.08
                
                # Factores que afectan la velocidad de escritura
                if i == 0:  # Primer caracter siempre más lento
                    delay = base_delay * initial_delay_multiplier
                elif char.lower() in 'aeiou':  # Vocales ligeramente más rápidas
                    delay = random.uniform(0.06, 0.14)
                elif char in '123456789':  # Números más deliberados
                    delay = random.uniform(0.10, 0.22)
                elif char.isupper():  # Mayúsculas más lentas (pensar en Shift)
                    delay = random.uniform(0.12, 0.25)
                else:
                    delay = random.uniform(0.07, 0.18)
                
                # Añadir variabilidad humana natural
                if random.random() < 0.15:  # 15% de las veces pausa extra
                    delay *= random.uniform(1.5, 2.5)
                
                time.sleep(delay)
            
            # Pausa después de escribir username (comportamiento natural)
            self.human_delay(1.2, 2.5)
            
            # === CLICK HUMANO EN BOTÓN "SIGUIENTE" ===
            self.logger.info("   🤔 Verificando username antes de continuar...")
            
            # Pausa realista antes de buscar el botón
            verification_delay = random.uniform(1.5, 3.0)
            self.logger.info(f"   ⏳ Pausa de verificación ({verification_delay:.1f}s)...")
            time.sleep(verification_delay)
            
            next_button = self.driver.find_element(By.ID, "username-submit-button")
            self.logger.info("   ➡️ Preparando click en 'Siguiente'...")
            
            # Movimiento gradual hacia el botón
            try:
                actions = ActionChains(self.driver)
                
                # Mover cerca del botón primero
                actions.move_to_element_with_offset(next_button, 
                                                  random.randint(-20, 20), 
                                                  random.randint(-10, 10))
                actions.pause(random.uniform(0.4, 0.8))
                
                # Mover al centro del botón
                actions.move_to_element(next_button)
                actions.pause(random.uniform(0.3, 0.7))
                
                # Click
                actions.click()
                actions.perform()
                
                self.logger.info("   ✅ Click humano en 'Siguiente' completado")
                
            except Exception as e:
                self.logger.warning(f"Error en click humano de Siguiente: {e}, usando fallback")
                self.human_click(next_button)
            
            self.human_delay(3, 6)  # Tiempo más realista para cargar siguiente página
            
            # Si ya tenemos el campo de contraseña, usarlo, sino buscarlo
            if password_field is None:
                self.logger.info("   🔍 Esperando campo de contraseña...")
                try:
                    password_field = WebDriverWait(self.driver, 15).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="password"], input[placeholder*="password"], input[placeholder*="Password"], input[id*="awsui-input"]'))
                    )
                except TimeoutException:
                    self.logger.warning("No se encontró campo de contraseña")
                    return False
            
            if password_field:
                self.logger.info("   🔐 Ingresando contraseña...")
                password_field.clear()
                self.human_delay(0.3, 0.7)  # Pausa antes de escribir
                
                # Escribir contraseña caracter por caracter con delays más humanos
                password = "ElSalvador5863_"
                for i, char in enumerate(password):
                    # Simular errores de escritura ocasionales (menos en contraseñas)
                    if random.random() < 0.05 and i > 3:  # 5% de probabilidad de error
                        wrong_char = random.choice('abcdefghijklmnopqrstuvwxyz123456789')
                        password_field.send_keys(wrong_char)
                        time.sleep(random.uniform(0.1, 0.3))
                        password_field.send_keys('\b')  # Backspace
                        time.sleep(random.uniform(0.05, 0.15))
                    
                    password_field.send_keys(char)
                    
                    # Delays variables más realistas para contraseñas
                    if char.lower() in 'aeiou':  # Vocales más rápidas
                        delay = random.uniform(0.06, 0.14)
                    elif char in '123456789':  # Números más lentos
                        delay = random.uniform(0.08, 0.20)
                    elif char in '_-@#$%':  # Símbolos más lentos
                        delay = random.uniform(0.10, 0.25)
                    else:
                        delay = random.uniform(0.07, 0.16)
                    
                    time.sleep(delay)
                
                # === PAUSA ULTRA REALISTA DESPUÉS DE ESCRIBIR CONTRASEÑA ===
                self.logger.info("   🤔 Revisando credenciales antes de enviar...")
                
                # Simular que el usuario "revisa" lo que escribió
                review_delay = random.uniform(2.5, 5.0)
                self.logger.info(f"   ⏳ Pausa de revisión ({review_delay:.1f}s)...")
                time.sleep(review_delay)
                
                # Movimiento de mouse como si estuviéramos "buscando" el botón
                try:
                    actions = ActionChains(self.driver)
                    # Mover cerca del botón pero no exactamente
                    actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), 
                                                      random.randint(400, 600), 
                                                      random.randint(400, 500))
                    actions.pause(random.uniform(0.8, 1.5))
                    actions.perform()
                except:
                    pass
                
                # === BÚSQUEDA EXHAUSTIVA DEL BOTÓN DE LOGIN ===
                self.logger.info("   🔍 Buscando botón de login con múltiples selectores...")
                
                # Múltiples selectores para encontrar el botón de login
                login_button_selectors = [
                    'button[type="submit"]',
                    'awsui-button button',
                    'button[data-testid*="submit"]',
                    'button[data-testid*="login"]',
                    'button[data-testid*="signin"]',
                    'button[aria-label*="Sign in"]',
                    'button[aria-label*="Login"]',
                    'button[value*="Sign in"]',
                    'input[type="submit"]',
                    'button:contains("Sign in")',
                    'button:contains("Login")',
                    '.awsui-button-variant-primary button',
                    '[data-testid="signin-button"]'
                ]
                
                submit_button = None
                
                # Intentar encontrar el botón con cada selector
                for selector in login_button_selectors:
                    try:
                        buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for button in buttons:
                            if button.is_displayed() and button.is_enabled():
                                # Verificar si el texto del botón sugiere que es el botón de login
                                button_text = button.text.lower()
                                button_value = button.get_attribute('value') or ""
                                button_aria = button.get_attribute('aria-label') or ""
                                
                                login_keywords = ['sign in', 'login', 'submit', 'continue', 'next', 'enter']
                                
                                if any(keyword in button_text or keyword in button_value.lower() or keyword in button_aria.lower() 
                                       for keyword in login_keywords):
                                    submit_button = button
                                    self.logger.info(f"   ✅ Botón de login encontrado con selector: {selector}")
                                    self.logger.info(f"   📝 Texto del botón: '{button_text}' | Valor: '{button_value}' | Aria: '{button_aria}'")
                                    break
                        
                        if submit_button:
                            break
                            
                    except Exception as e:
                        self.logger.debug(f"Selector {selector} falló: {e}")
                        continue
                
                # Si no encontramos un botón específico, usar el primer botón submit disponible
                if not submit_button:
                    try:
                        submit_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button[type="submit"], button')
                        for button in submit_buttons:
                            if button.is_displayed() and button.is_enabled():
                                submit_button = button
                                self.logger.info("   ⚠️ Usando primer botón disponible como fallback")
                                break
                    except:
                        pass
                
                if submit_button:
                    self.logger.info("   🎯 Botón de envío encontrado, preparando click ULTRA HUMANO...")
                    
                    # === SIMULACIÓN ULTRA REALISTA DE BÚSQUEDA Y CLICK ===
                    # Simular que el usuario está "buscando" el botón en la página
                    
                    # 1. Movimiento de exploración inicial
                    try:
                        actions = ActionChains(self.driver)
                        
                        # Simular que miramos alrededor de la página antes de encontrar el botón
                        exploration_points = [
                            (random.randint(200, 400), random.randint(300, 400)),
                            (random.randint(500, 700), random.randint(250, 350)),
                            (random.randint(300, 600), random.randint(400, 500))
                        ]
                        
                        self.logger.info("   👀 Simulando exploración visual de la página...")
                        for i, (x, y) in enumerate(exploration_points):
                            actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), x, y)
                            actions.pause(random.uniform(0.4, 0.8))
                            if i < len(exploration_points) - 1:
                                actions.pause(random.uniform(0.2, 0.5))
                        
                        actions.perform()
                        self.human_delay(0.8, 1.5)
                        
                        # 2. "Encontrar" el botón y aproximarse gradualmente
                        self.logger.info("   🎯 'Encontrando' el botón de login...")
                        
                        button_location = submit_button.location
                        button_size = submit_button.size
                        
                        # Calcular centro del botón
                        button_center_x = button_location['x'] + button_size['width'] // 2
                        button_center_y = button_location['y'] + button_size['height'] // 2
                        
                        # 3. Aproximación en múltiples pasos (muy humano)
                        actions = ActionChains(self.driver)
                        
                        # Paso 1: Mover cerca del botón (pero no exactamente)
                        approach_x = button_center_x + random.randint(-40, 40)
                        approach_y = button_center_y + random.randint(-30, 30)
                        actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), 
                                                          approach_x, approach_y)
                        actions.pause(random.uniform(0.6, 1.2))
                        
                        # Paso 2: Ajustar posición (como si estuviéramos "apuntando")
                        fine_tune_x = button_center_x + random.randint(-8, 8)
                        fine_tune_y = button_center_y + random.randint(-5, 5)
                        actions.move_to_element_with_offset(self.driver.find_element(By.TAG_NAME, "body"), 
                                                          fine_tune_x, fine_tune_y)
                        actions.pause(random.uniform(0.3, 0.6))
                        
                        # Paso 3: Posición final exacta en el botón
                        final_offset_x = random.randint(-button_size['width']//4, button_size['width']//4)
                        final_offset_y = random.randint(-button_size['height']//4, button_size['height']//4)
                        actions.move_to_element_with_offset(submit_button, final_offset_x, final_offset_y)
                        actions.pause(random.uniform(0.4, 0.8))
                        
                        # 4. Pausa de "decisión" antes del click
                        decision_delay = random.uniform(0.8, 1.8)
                        self.logger.info(f"   🤔 Pausa de decisión antes del click ({decision_delay:.1f}s)...")
                        actions.pause(decision_delay)
                        
                        # 5. Micro-movimiento final (comportamiento humano común)
                        micro_x = random.randint(-2, 2)
                        micro_y = random.randint(-2, 2)
                        actions.move_by_offset(micro_x, micro_y)
                        actions.pause(random.uniform(0.1, 0.3))
                        
                        # 6. CLICK FINAL
                        actions.click()
                        actions.perform()
                        
                        self.logger.info("   ✅ CLICK ULTRA HUMANO COMPLETADO EN BOTÓN DE LOGIN")
                        
                        # 7. Pausa post-click realista
                        post_click_delay = random.uniform(1.2, 2.5)
                        self.logger.info(f"   ⏳ Esperando respuesta del servidor ({post_click_delay:.1f}s)...")
                        time.sleep(post_click_delay)
                        
                    except Exception as e:
                        self.logger.warning(f"Error en click ultra humano: {e}, usando método de fallback")
                        
                        # === MÉTODO DE FALLBACK MEJORADO ===
                        try:
                            # Scroll para asegurar que el botón esté visible
                            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", submit_button)
                            self.human_delay(1, 2)
                            
                            # Usar JavaScript click como último recurso
                            self.driver.execute_script("arguments[0].click();", submit_button)
                            self.logger.info("   ✅ Fallback: Click con JavaScript completado")
                            
                            # Delay post-click
                            self.human_delay(2, 4)
                            
                        except Exception as fallback_error:
                            self.logger.error(f"Error en todos los métodos de click: {fallback_error}")
                            return False
                    
                else:
                    self.logger.error("   ❌ NO SE ENCONTRÓ NINGÚN BOTÓN DE LOGIN")
                    
                    # Debug: Mostrar todos los botones disponibles
                    try:
                        all_buttons = self.driver.find_elements(By.TAG_NAME, "button")
                        self.logger.info(f"   🔍 Botones disponibles en la página ({len(all_buttons)}):")
                        for i, btn in enumerate(all_buttons[:10]):  # Mostrar solo los primeros 10
                            try:
                                btn_text = btn.text.strip()
                                btn_type = btn.get_attribute('type')
                                btn_class = btn.get_attribute('class')
                                btn_id = btn.get_attribute('id')
                                self.logger.info(f"     {i+1}. Texto: '{btn_text}' | Tipo: '{btn_type}' | Clase: '{btn_class}' | ID: '{btn_id}'")
                            except:
                                pass
                    except:
                        pass
                    
                    return False
                
                # === VERIFICACIÓN POST-CLICK ULTRA INTELIGENTE ===
                self.logger.info("   🔍 Verificando que el click fue exitoso...")
                
                # Esperar un tiempo inicial para que la página procese el click
                initial_wait = random.uniform(2.0, 4.0)
                self.logger.info(f"   ⏳ Esperando procesamiento inicial ({initial_wait:.1f}s)...")
                time.sleep(initial_wait)
                
                # Verificar múltiples indicadores de éxito/fallo
                verification_attempts = 0
                max_verification_attempts = 15  # 15 intentos = ~45 segundos máximo
                login_successful = False
                
                while verification_attempts < max_verification_attempts and not login_successful:
                    verification_attempts += 1
                    
                    try:
                        current_url = self.driver.current_url.lower()
                        page_source = self.driver.page_source.lower()
                        page_title = self.driver.title.lower()
                        
                        # Indicadores de éxito
                        success_indicators = [
                            # URL cambió y ya no contiene login/signin
                            "login" not in current_url and "signin" not in current_url,
                            # Llegamos a dashboard o página principal
                            "dashboard" in current_url or "home" in current_url,
                            # El título cambió a algo relacionado con Grafana
                            "grafana" in page_title and "login" not in page_title,
                            # Elementos típicos de dashboard aparecieron
                            len(self.driver.find_elements(By.CSS_SELECTOR, '[data-testid*="dashboard"]')) > 0,
                            len(self.driver.find_elements(By.CSS_SELECTOR, '[data-testid*="search"]')) > 0,
                            # Desaparecieron elementos de login
                            len(self.driver.find_elements(By.CSS_SELECTOR, 'input[type="password"]')) == 0,
                        ]
                        
                        # Indicadores de fallo
                        failure_indicators = [
                            # Mensajes de error
                            "error" in page_source or "invalid" in page_source or "incorrect" in page_source,
                            # Todavía hay campos de login
                            len(self.driver.find_elements(By.CSS_SELECTOR, 'input[id*="awsui-input"]')) > 0,
                            # Botones de login todavía visibles
                            len([btn for btn in self.driver.find_elements(By.CSS_SELECTOR, 'button[type="submit"]') 
                                if btn.is_displayed()]) > 0,
                        ]
                        
                        # Evaluar resultado
                        success_score = sum(success_indicators)
                        failure_score = sum(failure_indicators)
                        
                        self.logger.info(f"   📊 Verificación {verification_attempts}/{max_verification_attempts}: "
                                      f"Éxito={success_score}/6, Fallo={failure_score}/3")
                        
                        if success_score >= 2 and failure_score == 0:
                            login_successful = True
                            self.logger.info("   ✅ LOGIN VERIFICADO COMO EXITOSO")
                            break
                        elif failure_score >= 2:
                            self.logger.warning("   ❌ LOGIN FALLÓ - Detectados indicadores de error")
                            break
                        elif verification_attempts >= 10 and success_score == 0:
                            self.logger.warning("   ⚠️ LOGIN POSIBLEMENTE FALLÓ - Sin indicadores de éxito después de 30s")
                            break
                        
                        # Esperar antes del próximo intento
                        time.sleep(3)
                        
                    except Exception as e:
                        self.logger.warning(f"Error en verificación {verification_attempts}: {e}")
                        time.sleep(2)
                        continue
                
                if login_successful:
                    # Pausa adicional para que la página cargue completamente
                    final_wait = random.uniform(3.0, 6.0)
                    self.logger.info(f"   🎉 Login exitoso confirmado, esperando carga completa ({final_wait:.1f}s)...")
                    time.sleep(final_wait)
                    return True
                else:
                    self.logger.error("   ❌ LOGIN NO PUDO SER VERIFICADO COMO EXITOSO")
                    
                    # === ESTRATEGIA DE RECUPERACIÓN: VERIFICAR SI NECESITAMOS REHACER EL CLICK ===
                    self.logger.info("   🔄 Intentando estrategia de recuperación...")
                    
                    try:
                        current_url = self.driver.current_url.lower()
                        page_source = self.driver.page_source.lower()
                        
                        # Si todavía estamos en una página de login, intentar rehacer el click
                        if ("login" in current_url or "signin" in current_url or 
                            len(self.driver.find_elements(By.CSS_SELECTOR, 'input[type="password"]')) > 0):
                            
                            self.logger.info("   🔍 Detectamos que todavía estamos en página de login, buscando botón nuevamente...")
                            
                            # Buscar botón de login otra vez con estrategia más agresiva
                            recovery_selectors = [
                                'button[type="submit"]',
                                'button',
                                'input[type="submit"]',
                                '[role="button"]',
                                'awsui-button',
                                '.awsui-button'
                            ]
                            
                            recovery_button = None
                            for selector in recovery_selectors:
                                try:
                                    buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                                    for btn in buttons:
                                        if btn.is_displayed() and btn.is_enabled():
                                            # Verificar si parece ser un botón de submit
                                            btn_text = btn.text.lower()
                                            btn_html = btn.get_attribute('outerHTML').lower()
                                            
                                            if (any(word in btn_text for word in ['sign', 'login', 'submit', 'continue', 'enter']) or
                                                any(word in btn_html for word in ['submit', 'signin', 'login'])):
                                                recovery_button = btn
                                                break
                                    if recovery_button:
                                        break
                                except:
                                    continue
                            
                            if recovery_button:
                                self.logger.info("   🎯 Botón de recuperación encontrado, intentando click...")
                                
                                try:
                                    # Scroll al botón
                                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", recovery_button)
                                    self.human_delay(1, 2)
                                    
                                    # Click con JavaScript (más confiable para recovery)
                                    self.driver.execute_script("arguments[0].click();", recovery_button)
                                    self.logger.info("   ✅ Click de recuperación ejecutado")
                                    
                                    # Esperar y verificar nuevamente
                                    recovery_wait = random.uniform(5, 10)
                                    self.logger.info(f"   ⏳ Esperando resultado de recuperación ({recovery_wait:.1f}s)...")
                                    time.sleep(recovery_wait)
                                    
                                    # Verificación rápida
                                    new_url = self.driver.current_url.lower()
                                    if "login" not in new_url and "signin" not in new_url:
                                        self.logger.info("   🎉 ¡Recuperación exitosa! Login completado")
                                        return True
                                    
                                except Exception as recovery_error:
                                    self.logger.warning(f"Error en click de recuperación: {recovery_error}")
                    
                    except Exception as recovery_exception:
                        self.logger.warning(f"Error en estrategia de recuperación: {recovery_exception}")
                    
                    # Debug adicional
                    try:
                        self.logger.info(f"   🔍 URL actual: {self.driver.current_url}")
                        self.logger.info(f"   🔍 Título actual: {self.driver.title}")
                        
                        # Listar elementos visibles que podrían ser botones
                        try:
                            visible_buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button, input[type="submit"], [role="button"]')
                            visible_count = len([btn for btn in visible_buttons if btn.is_displayed()])
                            self.logger.info(f"   🔍 Botones visibles en página: {visible_count}")
                        except:
                            pass
                        
                        # Tomar screenshot para debug si es posible
                        try:
                            screenshot_path = f"debug_login_failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                            self.driver.save_screenshot(screenshot_path)
                            self.logger.info(f"   📸 Screenshot guardado: {screenshot_path}")
                        except:
                            pass
                            
                    except:
                        pass
                    
                    return False
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error llenando credenciales: {e}")
            return False

    def login_sso(self):
        """Realiza el login mediante SSO de AWS IAM Identity Center con estrategia anti-detección avanzada"""
        max_attempts = 2  # Reducir intentos para no ser tan obvio
        
        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    self.logger.info(f"🔄 Reintento {attempt + 1}/{max_attempts}")
                
                self.logger.info("Navegando a la página de login...")
                self.driver.get(self.login_url)
                
                # Delay variable más realista
                initial_delay = random.uniform(3, 7)
                self.logger.info(f"   ⏳ Cargando página ({initial_delay:.1f}s)...")
                time.sleep(initial_delay)
                
                # Buscar y hacer click en el botón de SSO
                sso_button = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href="login/sso"]'))
                )
                
                self.logger.info("Botón SSO encontrado, haciendo click...")
                self.human_scroll(sso_button)
                
                if not self.human_click(sso_button):
                    # Fallback a click normal si el humano falla
                    sso_button.click()
                
                self.human_delay(3, 5)
                
                # Esperar a que aparezca la pantalla de login de AWS
                self.logger.info("Esperando pantalla de login de AWS IAM Identity Center...")
                
                # Esperar a que aparezca el formulario de username (IDs dinámicos)
                username_field = WebDriverWait(self.driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[id*="awsui-input"], #username-input input, [data-testid="test-input"] input'))
                )
                
                self.logger.info("🔐 PANTALLA DE LOGIN DETECTADA")
                
                # === LLENAR CREDENCIALES CON SISTEMA DE RETRY INTELIGENTE ===
                credential_success = False
                credential_attempts = 0
                max_credential_attempts = 3
                
                while not credential_success and credential_attempts < max_credential_attempts:
                    credential_attempts += 1
                    self.logger.info(f"   🔑 Intento de credenciales {credential_attempts}/{max_credential_attempts}")
                    
                    # Si no es el primer intento, aplicar estrategias diferentes
                    if credential_attempts > 1:
                        self.logger.info("   🔄 Aplicando estrategia de recuperación...")
                        
                        # Estrategia 1: Refrescar la página y empezar de nuevo
                        if credential_attempts == 2:
                            self.logger.info("   🔄 Refrescando página para limpiar estado...")
                            self.driver.refresh()
                            self.human_delay(4, 8)
                            
                            # Re-buscar el campo de username después del refresh
                            try:
                                username_field = WebDriverWait(self.driver, 15).until(
                                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[id*="awsui-input"], #username-input input, [data-testid="test-input"] input'))
                                )
                            except TimeoutException:
                                self.logger.warning("No se pudo encontrar campo de username después del refresh")
                                continue
                        
                        # Estrategia 2: Cambiar método de interacción
                        elif credential_attempts == 3:
                            self.logger.info("   🎯 Usando método de interacción alternativo...")
                            # Usar JavaScript para limpiar campos
                            try:
                                self.driver.execute_script("document.querySelectorAll('input').forEach(input => input.value = '');")
                                self.human_delay(1, 2)
                            except:
                                pass
                    
                    # Intentar llenar credenciales
                    try:
                        credential_success = self.fill_credentials_human(username_field)
                        
                        if credential_success:
                            self.logger.info(f"   ✅ Credenciales llenadas exitosamente en intento {credential_attempts}")
                            break
                        else:
                            if credential_attempts < max_credential_attempts:
                                retry_delay = random.uniform(3, 8)
                                self.logger.warning(f"   ⚠️ Fallo en intento {credential_attempts}, reintentando en {retry_delay:.1f}s...")
                                time.sleep(retry_delay)
                            else:
                                self.logger.error("   ❌ Todos los intentos de credenciales fallaron")
                                
                    except Exception as cred_error:
                        self.logger.error(f"   ❌ Error en intento de credenciales {credential_attempts}: {cred_error}")
                        if credential_attempts < max_credential_attempts:
                            self.human_delay(2, 5)
                        continue
                
                # Si no logramos llenar credenciales después de todos los intentos
                if not credential_success:
                    if attempt < max_attempts - 1:
                        self.logger.warning("❌ Fallo llenando credenciales después de todos los reintentos, reiniciando sesión...")
                        continue
                    else:
                        self.logger.error("❌ Fallo crítico: No se pudieron llenar credenciales después de todos los intentos")
                        return False
                
                self.logger.info("⏳ Esperando completar proceso de autenticación...")
                
                # === ESTRATEGIA AVANZADA: ESPERA INTELIGENTE CON MÚLTIPLES CONDICIONES ===
                self.logger.info("🔍 Verificando estado de autenticación...")
                
                # Esperar con verificaciones más inteligentes
                success = False
                wait_time = 0
                max_wait = 60  # 60 segundos máximo
                
                while wait_time < max_wait and not success:
                    self.human_delay(2, 4)  # Espera realista entre verificaciones
                    wait_time += 3
                    
                    # Verificar múltiples condiciones de éxito
                    current_url = self.driver.current_url.lower()
                    page_source = self.driver.page_source.lower()
                    
                    success_conditions = [
                        "/dashboards" in current_url,
                        "dashboard" in self.driver.title.lower(),
                        "grafana" in current_url and "login" not in current_url,
                        len(self.driver.find_elements(By.CSS_SELECTOR, '[data-testid*="Dashboard search item"]')) > 0,
                        len(self.driver.find_elements(By.CSS_SELECTOR, '[data-testid*="Search section"]')) > 0,
                        len(self.driver.find_elements(By.CSS_SELECTOR, '.css-1rf724w')) > 0,
                        "welcome" in page_source and "dashboard" in page_source
                    ]
                    
                    if any(success_conditions):
                        success = True
                        self.logger.info("✅ Autenticación completada exitosamente")
                        self.human_delay(3, 6)  # Tiempo para que cargue completamente
                        break
                    
                    # Verificar si hay errores o necesidad de reintento
                    error_conditions = [
                        "error" in page_source,
                        "invalid" in page_source,
                        "incorrect" in page_source,
                        len(self.driver.find_elements(By.CSS_SELECTOR, 'input[id*="awsui-input"]')) > 0
                    ]
                    
                    if any(error_conditions) and wait_time > 20:
                        self.logger.warning("⚠️ Detectados posibles errores de autenticación")
                        break
                    
                    if wait_time % 15 == 0:  # Log cada 15 segundos
                        self.logger.info(f"   ⏳ Esperando autenticación... ({wait_time}s/{max_wait}s)")
                
                if success:
                    self.logger.info("🚀 Continuando con extracción de dashboards...")
                    return True
                else:
                    # Si no tuvo éxito, continuar al manejo de timeout
                    pass
                
            except TimeoutException:
                if attempt < max_attempts - 1:
                    self.logger.warning(f"❌ Timeout en intento {attempt + 1}, reintentando con nueva sesión...")
                    
                    # === ESTRATEGIA NUEVA: REINICIAR SESIÓN COMPLETA ===
                    self.logger.info("🔄 AWS detectó comportamiento automatizado, cambiando identidad completa...")
                    
                    # Reiniciar sesión con nueva identidad
                    self.restart_session()
                    
                    # Continuar con el siguiente intento (el for loop se encargará)
                    continue
                else:
                    self.logger.error("❌ Timeout esperando completar SSO después de todos los reintentos")
                    self.logger.info("💡 Posibles causas:")
                    self.logger.info("   - Credenciales incorrectas")
                    self.logger.info("   - Problema con la autenticación de dos factores")
                    self.logger.info("   - Conexión de red lenta")
                    return False
                
            except Exception as e:
                if attempt < max_attempts - 1:
                    self.logger.error(f"Error en intento {attempt + 1}: {e}, reintentando...")
                    continue
                else:
                    self.logger.error(f"Error durante login SSO después de todos los reintentos: {e}")
                    return False
        
        return False
            
    def extract_dashboard_urls(self):
        """Extrae todas las URLs de dashboards de la página principal"""
        try:
            self.logger.info("Navegando a página de dashboards...")
            self.driver.get(self.dashboards_url)
            self.human_delay(3, 5)
            
            # Esperar a que cargue la página
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid*="Dashboard search item"]'))
            )
            
            dashboard_urls = []
            
            # Buscar todos los enlaces de dashboards
            dashboard_links = self.driver.find_elements(By.CSS_SELECTOR, '[data-testid*="Dashboard search item"] h2 a')
            
            for link in dashboard_links:
                href = link.get_attribute('href')
                title = link.text.strip()
                if href and title:
                    dashboard_urls.append({
                        'title': title,
                        'url': href,
                        'relative_url': href.replace(self.base_url, '')
                    })
                    self.logger.info(f"Dashboard encontrado: {title}")
            
            self.dashboard_urls = dashboard_urls
            self.logger.info(f"Total de dashboards encontrados: {len(dashboard_urls)}")
            
            return dashboard_urls
            
        except Exception as e:
            self.logger.error(f"Error extrayendo URLs de dashboards: {e}")
            return []
            
    def extract_dashboard_data(self, dashboard_info):
        """Extrae datos reales de un dashboard específico"""
        try:
            self.logger.info(f"Extrayendo datos del dashboard: {dashboard_info['title']}")
            
            self.driver.get(dashboard_info['url'])
            self.human_delay(3, 6)
            
            # Esperar a que cargue el dashboard
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.react-grid-layout, .dashboard-container, [data-testid="dashboard-grid"]'))
            )
            
            # === ESPERAR A QUE LOS DATOS SE CARGUEN ===
            self.logger.info("⏳ Esperando a que los paneles carguen datos...")
            
            # Esperar tiempo adicional para que los datos se carguen
            initial_load_time = random.uniform(8, 15)
            self.logger.info(f"   ⏳ Esperando carga inicial ({initial_load_time:.1f}s)...")
            time.sleep(initial_load_time)
            
            # === ACTIVAR CARGA DE DATOS CON INTERACCIONES ===
            self.logger.info("🖱️ Activando carga de datos con interacciones...")
            
            # Scroll completo para activar lazy loading
            self.driver.execute_script("window.scrollTo(0, 0);")
            self.human_delay(1, 2)
            
            # Scroll gradual hacia abajo para activar todos los paneles
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            scroll_step = 300
            
            while current_position < total_height:
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                self.human_delay(1, 2)  # Tiempo para que carguen los datos
            
            # Volver arriba
            self.driver.execute_script("window.scrollTo(0, 0);")
            self.human_delay(2, 4)
            
            # === INTERACCIÓN CON PANELES PARA ACTIVAR DATOS ===
            self.logger.info("🎯 Interactuando con paneles para activar datos...")
            
            panels = self.driver.find_elements(By.CSS_SELECTOR, '.react-grid-item')
            
            # Hacer hover sobre cada panel para activar datos
            for i, panel in enumerate(panels[:10]):  # Limitar a primeros 10 para no ser muy lento
                try:
                    actions = ActionChains(self.driver)
                    actions.move_to_element(panel)
                    actions.pause(random.uniform(0.5, 1.0))
                    actions.perform()
                    self.human_delay(0.5, 1.0)
                except:
                    continue
            
            # Tiempo final para que todos los datos se carguen
            final_wait = random.uniform(10, 20)
            self.logger.info(f"⏳ Esperando carga final de datos ({final_wait:.1f}s)...")
            time.sleep(final_wait)
            
            # === ESTRUCTURA DE DATOS MEJORADA ===
            dashboard_data = {
                'dashboard_title': dashboard_info['title'],
                'url': dashboard_info['url'],
                'extracted_at': datetime.now().isoformat(),
                'availability_data': {},
                'summary_data': {},
                'charts_data': {},
                'tables_data': {},
                'payment_methods': {},
                'totals': {}
            }
            
            # === EXTRAER DATOS REALES DE PANELES ===
            self.logger.info("📊 Extrayendo datos reales de paneles...")
            
            panels = self.driver.find_elements(By.CSS_SELECTOR, '.react-grid-item')
            
            for i, panel in enumerate(panels):
                try:
                    self.logger.info(f"   📋 Procesando panel {i+1}/{len(panels)}...")
                    
                    # Scroll al panel para asegurar visibilidad
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", panel)
                    self.human_delay(1, 2)
                    
                    panel_data = self.extract_real_panel_data(panel, i)
                    if panel_data:
                        # Organizar datos por tipo
                        panel_type = panel_data.get('type', 'unknown')
                        panel_title = panel_data.get('title', f'panel_{i}')
                        
                        if panel_type == 'stat' and panel_data.get('values'):
                            # Datos de disponibilidad (números grandes)
                            if any(keyword in panel_title.lower() for keyword in ['total', 'vendidas', 'disponibles', 'reservadas']):
                                dashboard_data['availability_data'][panel_title] = {
                                    'values': panel_data['values'],
                                    'labels': panel_data.get('labels', []),
                                    'type': panel_data.get('stat_type', 'unknown')
                                }
                        
                        elif panel_type == 'chart':
                            # Datos de gráficos
                            dashboard_data['charts_data'][panel_title or f'chart_{i}'] = {
                                'chart_type': panel_data.get('chart_type', 'unknown'),
                                'data_points': panel_data.get('data_points', []),
                                'legend_items': panel_data.get('legend_items', []),
                                'axes_labels': panel_data.get('axes_labels', [])
                            }
                        
                        elif panel_type == 'table' and panel_data.get('rows'):
                            # Datos de tablas
                            dashboard_data['tables_data'][panel_title or f'table_{i}'] = {
                                'headers': panel_data.get('headers', []),
                                'rows': panel_data['rows'],
                                'total_rows': len(panel_data['rows'])
                            }
                    
                    self.human_delay(0.5, 1.0)
                    
                except Exception as e:
                    self.logger.warning(f"Error extrayendo panel {i}: {e}")
                    continue
            
            # === EXTRAER DATOS AGREGADOS ===
            self.extract_aggregated_data(dashboard_data)
            
            # Limpiar datos vacíos PERO mantener metadatos esenciales
            essential_fields = ['dashboard_title', 'url', 'extracted_at']
            cleaned_data = {}
            
            # Mantener campos esenciales siempre
            for field in essential_fields:
                if field in dashboard_data:
                    cleaned_data[field] = dashboard_data[field]
            
            # Mantener solo campos de datos que tengan contenido
            for k, v in dashboard_data.items():
                if k not in essential_fields and v:  # Solo campos con datos
                    cleaned_data[k] = v
            
            dashboard_data = cleaned_data
            
            self.logger.info(f"✅ Datos extraídos: {len(dashboard_data.get('availability_data', {}))} métricas de disponibilidad, "
                           f"{len(dashboard_data.get('charts_data', {}))} gráficos, "
                           f"{len(dashboard_data.get('tables_data', {}))} tablas")
            
            return dashboard_data
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos del dashboard {dashboard_info['title']}: {e}")
            return None
            
    def extract_panel_data(self, panel, index):
        """Extrae datos de un panel específico con detección avanzada"""
        try:
            panel_data = {
                'index': index,
                'panel_id': '',
                'type': 'unknown',
                'title': '',
                'data': {},
                'text_content': [],
                'metadata': {},
                'html_content': '',
                'status': 'unknown'
            }
            
            # Extraer panel ID desde el atributo data-panelid
            try:
                panel_id = panel.get_attribute('data-panelid')
                if panel_id:
                    panel_data['panel_id'] = panel_id
            except:
                pass
            
            # === BÚSQUEDA EXHAUSTIVA DE TÍTULOS ===
            title_selectors = [
                '.css-1gy0560-panel-title',
                '[data-testid="header-container"] h6',
                '.panel-title',
                'h6[title]',
                '[aria-label*="panel"] h6',
                '.dashboard-row__title',
                '[data-testid*="dashboard-row-title"]'
            ]
            
            for selector in title_selectors:
                try:
                    title_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                    for title_element in title_elements:
                        title_text = title_element.text.strip()
                        title_attr = title_element.get_attribute('title')
                        
                        if title_text and title_text not in ['Menu', '']:
                            panel_data['title'] = title_text
                            break
                        elif title_attr and title_attr not in ['Menu', '']:
                            panel_data['title'] = title_attr
                            break
                    
                    if panel_data['title']:
                        break
                except:
                    continue
            
            # === DETECCIÓN DE ESTADO DEL PANEL ===
            # Verificar si el panel tiene datos o está vacío
            no_data_indicators = [
                'No data',
                'no data',
                'Sin datos',
                'Loading...',
                'Cargando...'
            ]
            
            panel_html = panel.get_attribute('outerHTML')
            panel_text = panel.text.lower()
            
            for indicator in no_data_indicators:
                if indicator.lower() in panel_text:
                    panel_data['status'] = 'no_data'
                    break
            else:
                panel_data['status'] = 'has_data'
            
            # === EXTRACCIÓN MEJORADA DE CONTENIDO DE TEXTO ===
            text_selectors = [
                '.css-1w5pd0q',
                '.css-18o2w4z', 
                '[role="cell"]',
                '[role="gridcell"]',
                '.panel-content',
                '.css-jv7tdc',
                '.value',
                '.metric-value',
                'span[style*="font-size"]',
                '.stat-panel-value'
            ]
            
            for selector in text_selectors:
                try:
                    text_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                    for element in text_elements:
                        text = element.text.strip()
                        if text and text not in panel_data['text_content'] and len(text) > 0:
                            # Filtrar textos no útiles
                            if text not in ['Menu', 'No data', 'Loading...', '...']:
                                panel_data['text_content'].append(text)
                except:
                    continue
            
            # === DETECCIÓN AVANZADA DE TIPO DE PANEL ===
            
            # 1. Verificar si es un dashboard row (sección)
            if panel.find_elements(By.CSS_SELECTOR, '.dashboard-row'):
                panel_data['type'] = 'dashboard_row'
                # Extraer información de la fila
                row_info = self.extract_dashboard_row_data(panel)
                panel_data['data'] = row_info
            
            # 2. Verificar si es una tabla
            elif panel.find_elements(By.CSS_SELECTOR, '[role="table"], table, .table'):
                panel_data['type'] = 'table'
                panel_data['data'] = self.extract_table_data(panel)
            
            # 3. Verificar si es un gráfico (canvas)
            elif panel.find_elements(By.CSS_SELECTOR, 'canvas'):
                panel_data['type'] = 'chart'
                panel_data['data'] = self.extract_chart_data(panel)
            
            # 4. Verificar si es una estadística/métrica
            elif (panel.find_elements(By.CSS_SELECTOR, '.css-1w5pd0q') or 
                  panel.find_elements(By.CSS_SELECTOR, 'span[style*="font-size"]')):
                panel_data['type'] = 'stat'
                panel_data['data'] = self.extract_stat_data(panel)
            
            # 5. Verificar si es un panel de texto
            elif panel.find_elements(By.CSS_SELECTOR, '.css-18o2w4z'):
                panel_data['type'] = 'text'
                panel_data['data'] = {'content': panel_data['text_content']}
            
            # === EXTRACCIÓN DE METADATOS ADICIONALES ===
            try:
                # Dimensiones del panel
                panel_data['metadata']['width'] = panel.size['width']
                panel_data['metadata']['height'] = panel.size['height']
                
                # Posición del panel
                location = panel.location
                panel_data['metadata']['position'] = {
                    'x': location['x'],
                    'y': location['y']
                }
                
                # Clases CSS del panel
                panel_classes = panel.get_attribute('class')
                if panel_classes:
                    panel_data['metadata']['css_classes'] = panel_classes
                    
            except:
                pass
            
            # === GUARDAR HTML PARA DEBUG (OPCIONAL) ===
            if self.logger.level <= 10:  # Solo en modo DEBUG
                try:
                    panel_data['html_content'] = panel.get_attribute('innerHTML')[:500]  # Primeros 500 chars
                except:
                    pass
            
            # Retornar datos solo si el panel tiene información útil
            has_useful_data = (
                panel_data['title'] or 
                panel_data['text_content'] or 
                panel_data['data'] or
                panel_data['type'] != 'unknown'
            )
            
            if has_useful_data:
                self.logger.info(f"   📊 Panel {index}: '{panel_data['title']}' | Tipo: {panel_data['type']} | Estado: {panel_data['status']}")
                return panel_data
            else:
                self.logger.debug(f"   ⚪ Panel {index}: Sin datos útiles, omitiendo")
                return None
            
        except Exception as e:
            self.logger.warning(f"Error extrayendo datos del panel {index}: {e}")
            return None
    
    def extract_real_panel_data(self, panel, index):
        """Extrae datos REALES de un panel (números, valores, contenido)"""
        try:
            panel_data = {
                'index': index,
                'type': 'unknown',
                'title': '',
                'values': [],
                'labels': [],
                'chart_type': 'unknown',
                'data_points': [],
                'legend_items': [],
                'axes_labels': [],
                'headers': [],
                'rows': [],
                'stat_type': 'unknown'
            }
            
            # === EXTRAER TÍTULO ===
            title_selectors = [
                '.css-1gy0560-panel-title',
                'h6[title]',
                '[data-testid="header-container"] h6',
                '.panel-title'
            ]
            
            for selector in title_selectors:
                try:
                    title_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                    for element in title_elements:
                        title = element.text.strip() or element.get_attribute('title')
                        if title and title not in ['Menu', '']:
                            panel_data['title'] = title
                            break
                    if panel_data['title']:
                        break
                except:
                    continue
            
            # === DETECTAR Y EXTRAER VALORES NUMÉRICOS GRANDES (STATS) ===
            # Buscar números grandes que son las métricas principales
            large_number_selectors = [
                'span[style*="font-size: 48px"]',
                'span[style*="font-size: 40px"]', 
                'span[style*="font-size: 36px"]',
                'span[style*="font-size: 32px"]',
                '[style*="font-size"][style*="px"] span',
                '.css-1w5pd0q',
                '.stat-panel-value',
                '.single-stat-value'
            ]
            
            for selector in large_number_selectors:
                try:
                    elements = panel.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.strip()
                        if text and len(text) > 0:
                            # Verificar si es un número o contiene números
                            if any(c.isdigit() for c in text):
                                # Extraer información de estilo para priorizar números grandes
                                style = element.get_attribute('style') or ''
                                font_size = self.extract_font_size(style)
                                
                                if font_size >= 24 or text.replace(',', '').replace('.', '').isdigit():
                                    if text not in panel_data['values']:
                                        panel_data['values'].append(text)
                                        panel_data['type'] = 'stat'
                except:
                    continue
            
            # === EXTRAER DATOS DE GRÁFICOS ===
            canvas_elements = panel.find_elements(By.CSS_SELECTOR, 'canvas')
            if canvas_elements:
                panel_data['type'] = 'chart'
                
                # Buscar leyenda
                legend_selectors = [
                    '.css-r199cl-LegendLabel',
                    '[class*="legend"]',
                    '.legend-item',
                    '.viz-legend span'
                ]
                
                for selector in legend_selectors:
                    try:
                        legend_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                        for element in legend_elements:
                            text = element.text.strip()
                            if text and text not in panel_data['legend_items']:
                                panel_data['legend_items'].append(text)
                    except:
                        continue
                
                # Buscar valores numéricos en el contexto del gráfico
                numeric_elements = panel.find_elements(By.CSS_SELECTOR, 'text, span, div')
                for element in numeric_elements:
                    try:
                        text = element.text.strip()
                        if text and len(text) < 20 and any(c.isdigit() for c in text):
                            if text not in panel_data['data_points']:
                                panel_data['data_points'].append(text)
                    except:
                        continue
            
            # === EXTRAER DATOS DE TABLAS ===
            table_indicators = panel.find_elements(By.CSS_SELECTOR, '[role="table"], table, .table')
            if table_indicators:
                panel_data['type'] = 'table'
                
                # Headers
                header_selectors = [
                    '[role="columnheader"]',
                    'th',
                    '.table-header'
                ]
                
                for selector in header_selectors:
                    try:
                        headers = panel.find_elements(By.CSS_SELECTOR, selector)
                        for header in headers:
                            text = header.text.strip()
                            if text and text not in panel_data['headers']:
                                panel_data['headers'].append(text)
                    except:
                        continue
                
                # Rows
                row_selectors = [
                    '[role="row"]:not([aria-label*="header"])',
                    'tr:not(.header)',
                    '.table-row'
                ]
                
                for selector in row_selectors:
                    try:
                        rows = panel.find_elements(By.CSS_SELECTOR, selector)
                        for row in rows:
                            cells = row.find_elements(By.CSS_SELECTOR, '[role="cell"], td, .cell')
                            row_data = []
                            for cell in cells:
                                cell_text = cell.text.strip()
                                if cell_text:
                                    row_data.append(cell_text)
                            if row_data:
                                panel_data['rows'].append(row_data)
                    except:
                        continue
            
            # === EXTRAER TODOS LOS TEXTOS VISIBLES COMO FALLBACK ===
            if not panel_data['values'] and not panel_data['legend_items'] and not panel_data['rows']:
                # Buscar cualquier texto visible que contenga números
                all_elements = panel.find_elements(By.CSS_SELECTOR, 'span, div, text, tspan')
                for element in all_elements:
                    try:
                        if element.is_displayed():
                            text = element.text.strip()
                            if text and len(text) < 100 and any(c.isdigit() for c in text):
                                # Verificar que no sea un texto decorativo
                                if not any(word in text.lower() for word in ['menu', 'loading', 'error', 'null']):
                                    if text not in panel_data['values']:
                                        panel_data['values'].append(text)
                    except:
                        continue
            
            # Retornar solo si tiene datos útiles
            has_data = (
                panel_data['values'] or 
                panel_data['legend_items'] or 
                panel_data['rows'] or 
                panel_data['data_points']
            )
            
            if has_data:
                return panel_data
            else:
                return None
                
        except Exception as e:
            self.logger.warning(f"Error extrayendo datos reales del panel {index}: {e}")
            return None
    
    def extract_font_size(self, style_string):
        """Extrae el tamaño de fuente de un string de estilo CSS"""
        try:
            import re
            match = re.search(r'font-size:\s*(\d+)px', style_string)
            if match:
                return int(match.group(1))
            return 0
        except:
            return 0
    
    def extract_aggregated_data(self, dashboard_data):
        """Extrae datos agregados del dashboard completo"""
        try:
            # Buscar elementos con números grandes en toda la página
            large_numbers = self.driver.find_elements(By.CSS_SELECTOR, 
                'span[style*="font-size"], .css-1w5pd0q, [class*="stat"], [class*="metric"]')
            
            aggregated_values = {}
            
            for element in large_numbers:
                try:
                    if element.is_displayed():
                        text = element.text.strip()
                        if text and any(c.isdigit() for c in text):
                            # Buscar contexto (título del panel padre)
                            try:
                                parent = element.find_element(By.XPATH, 
                                    './ancestor::*[contains(@class, "react-grid-item") or contains(@class, "panel")]')
                                title_element = parent.find_elements(By.CSS_SELECTOR, 
                                    '.css-1gy0560-panel-title, h6, .panel-title')
                                context = title_element[0].text.strip() if title_element else 'unknown'
                                
                                if context not in aggregated_values:
                                    aggregated_values[context] = []
                                
                                if text not in aggregated_values[context]:
                                    aggregated_values[context].append(text)
                                    
                            except:
                                if 'general' not in aggregated_values:
                                    aggregated_values['general'] = []
                                if text not in aggregated_values['general']:
                                    aggregated_values['general'].append(text)
                except:
                    continue
            
            # Agregar a los datos del dashboard
            if aggregated_values:
                dashboard_data['aggregated_metrics'] = aggregated_values
                
        except Exception as e:
            self.logger.warning(f"Error extrayendo datos agregados: {e}")
    
    def extract_dashboard_row_data(self, panel):
        """Extrae datos de filas de dashboard (secciones)"""
        try:
            row_data = {
                'type': 'dashboard_row',
                'title': '',
                'panel_count': 0,
                'is_collapsed': False,
                'section_info': ''
            }
            
            # Extraer título de la fila
            title_element = panel.find_elements(By.CSS_SELECTOR, '.dashboard-row__title, [data-testid*="dashboard-row-title"]')
            if title_element:
                row_data['title'] = title_element[0].text.strip()
            
            # Extraer información de paneles en la sección
            panel_count_element = panel.find_elements(By.CSS_SELECTOR, '.dashboard-row__panel_count')
            if panel_count_element:
                panel_count_text = panel_count_element[0].text.strip()
                row_data['section_info'] = panel_count_text
                
                # Extraer número de paneles
                import re
                count_match = re.search(r'\((\d+) panels?\)', panel_count_text)
                if count_match:
                    row_data['panel_count'] = int(count_match.group(1))
            
            # Verificar si la fila está colapsada (por el ícono)
            collapse_icon = panel.find_elements(By.CSS_SELECTOR, 'svg')
            if collapse_icon:
                # Si encuentra el ícono de flecha hacia abajo, está expandida
                svg_html = collapse_icon[0].get_attribute('outerHTML')
                if 'M17,9.17' in svg_html:  # Flecha hacia abajo
                    row_data['is_collapsed'] = False
                else:
                    row_data['is_collapsed'] = True
            
            return row_data
            
        except Exception as e:
            self.logger.warning(f"Error extrayendo datos de dashboard row: {e}")
            return {'type': 'dashboard_row', 'title': '', 'panel_count': 0}
            
    def extract_chart_data(self, panel):
        """Extrae datos de gráficos con detección avanzada"""
        chart_data = {
            'has_canvas': True,
            'legend_items': [],
            'chart_type': 'unknown',
            'data_points': [],
            'axes_labels': [],
            'tooltips': [],
            'canvas_info': {}
        }
        
        try:
            # === INFORMACIÓN DEL CANVAS ===
            canvas_elements = panel.find_elements(By.CSS_SELECTOR, 'canvas')
            for i, canvas in enumerate(canvas_elements):
                canvas_info = {
                    'index': i,
                    'width': canvas.get_attribute('width'),
                    'height': canvas.get_attribute('height'),
                    'style': canvas.get_attribute('style')
                }
                chart_data['canvas_info'][f'canvas_{i}'] = canvas_info
            
            # === EXTRACCIÓN DE LEYENDA MEJORADA ===
            legend_selectors = [
                '.css-r199cl-LegendLabel',
                '[aria-label*="VizLegend"]',
                '.legend-item',
                '.legend-label',
                '[class*="legend"]',
                '.viz-legend',
                '.graph-legend'
            ]
            
            for selector in legend_selectors:
                legend_items = panel.find_elements(By.CSS_SELECTOR, selector)
                for item in legend_items:
                    text = item.text.strip()
                    if text and text not in chart_data['legend_items']:
                        chart_data['legend_items'].append(text)
            
            # === DETECCIÓN DE TIPO DE GRÁFICO ===
            panel_html = panel.get_attribute('outerHTML').lower()
            if 'line' in panel_html:
                chart_data['chart_type'] = 'line'
            elif 'bar' in panel_html:
                chart_data['chart_type'] = 'bar'
            elif 'pie' in panel_html:
                chart_data['chart_type'] = 'pie'
            elif 'scatter' in panel_html:
                chart_data['chart_type'] = 'scatter'
            else:
                chart_data['chart_type'] = 'unknown'
            
            # === EXTRACCIÓN DE ETIQUETAS DE EJES ===
            axis_selectors = [
                '[class*="axis"]',
                '[class*="label"]',
                '.tick-text',
                '.axis-label'
            ]
            
            for selector in axis_selectors:
                axis_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                for element in axis_elements:
                    text = element.text.strip()
                    if text and len(text) < 50:  # Evitar textos muy largos
                        chart_data['axes_labels'].append(text)
            
            # === INTENTAR EXTRAER DATOS DE TOOLTIPS (SI ESTÁN VISIBLES) ===
            tooltip_selectors = [
                '[class*="tooltip"]',
                '[role="tooltip"]',
                '.graph-tooltip'
            ]
            
            for selector in tooltip_selectors:
                tooltip_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                for element in tooltip_elements:
                    if element.is_displayed():
                        text = element.text.strip()
                        if text:
                            chart_data['tooltips'].append(text)
            
            # === BUSCAR VALORES NUMÉRICOS EN EL CONTEXTO DEL GRÁFICO ===
            numeric_elements = panel.find_elements(By.CSS_SELECTOR, 'text, tspan, span')
            for element in numeric_elements:
                text = element.text.strip()
                if text and (text.replace('.', '').replace(',', '').replace('-', '').isdigit() or 
                           any(char.isdigit() for char in text)):
                    if len(text) < 20 and text not in chart_data['data_points']:
                        chart_data['data_points'].append(text)
            
        except Exception as e:
            self.logger.warning(f"Error en extracción avanzada de chart: {e}")
        
        return chart_data
        
    def extract_table_data(self, panel):
        """Extrae datos de tablas con detección mejorada"""
        table_data = {
            'headers': [],
            'rows': [],
            'table_type': 'unknown',
            'total_rows': 0,
            'total_columns': 0,
            'has_pagination': False,
            'table_metadata': {}
        }
        
        try:
            # === DETECCIÓN DE MÚLTIPLES TIPOS DE TABLA ===
            
            # Buscar tablas con diferentes estructuras
            table_selectors = [
                '[role="table"]',
                'table',
                '.table',
                '.data-table',
                '.grid-table'
            ]
            
            table_element = None
            for selector in table_selectors:
                tables = panel.find_elements(By.CSS_SELECTOR, selector)
                if tables:
                    table_element = tables[0]
                    table_data['table_type'] = selector
                    break
            
            if not table_element:
                # Si no hay tabla formal, buscar estructuras tipo grid
                table_element = panel
                table_data['table_type'] = 'grid'
            
            # === EXTRACCIÓN DE HEADERS MEJORADA ===
            header_selectors = [
                '[role="columnheader"]',
                '[role="columnheader"] div',
                '.css-1n3c59t div',
                'th',
                '.table-header',
                '.column-header',
                '[data-testid*="header"]'
            ]
            
            for selector in header_selectors:
                headers = table_element.find_elements(By.CSS_SELECTOR, selector)
                for header in headers:
                    text = header.text.strip()
                    if text and text not in table_data['headers'] and len(text) < 100:
                        table_data['headers'].append(text)
                
                if table_data['headers']:  # Si ya encontramos headers, no seguir buscando
                    break
            
            # === EXTRACCIÓN DE FILAS MEJORADA ===
            row_selectors = [
                '[role="row"]:not([aria-label="table header"])',
                '[role="row"]',
                'tr:not(.header)',
                'tr',
                '.table-row',
                '.data-row'
            ]
            
            for selector in row_selectors:
                rows = table_element.find_elements(By.CSS_SELECTOR, selector)
                
                for row in rows:
                    # Verificar que no sea header
                    if 'header' in row.get_attribute('class').lower():
                        continue
                        
                    # Extraer celdas con múltiples selectores
                    cell_selectors = [
                        '[role="cell"]',
                        '[role="gridcell"]',
                        'td',
                        '.table-cell',
                        '.cell'
                    ]
                    
                    row_data = []
                    for cell_selector in cell_selectors:
                        cells = row.find_elements(By.CSS_SELECTOR, cell_selector)
                        if cells:
                            for cell in cells:
                                # Buscar texto en diferentes niveles
                                cell_text = cell.text.strip()
                                if not cell_text:
                                    # Buscar en elementos hijos
                                    inner_elements = cell.find_elements(By.CSS_SELECTOR, '.css-1w5pd0q, span, div')
                                    for inner in inner_elements:
                                        inner_text = inner.text.strip()
                                        if inner_text:
                                            cell_text = inner_text
                                            break
                                
                                if cell_text:
                                    row_data.append(cell_text)
                            break  # Si encontramos celdas con un selector, no probar otros
                    
                    if row_data and len(row_data) > 0:
                        table_data['rows'].append(row_data)
                
                if table_data['rows']:  # Si ya encontramos filas, no seguir buscando
                    break
            
            # === METADATOS DE LA TABLA ===
            table_data['total_rows'] = len(table_data['rows'])
            table_data['total_columns'] = len(table_data['headers']) if table_data['headers'] else (max(len(row) for row in table_data['rows']) if table_data['rows'] else 0)
            
            # Verificar paginación
            pagination_elements = panel.find_elements(By.CSS_SELECTOR, '.pagination, [class*="page"], [aria-label*="page"]')
            table_data['has_pagination'] = len(pagination_elements) > 0
            
            # Información adicional
            table_data['table_metadata'] = {
                'has_headers': len(table_data['headers']) > 0,
                'max_row_length': max(len(row) for row in table_data['rows']) if table_data['rows'] else 0,
                'min_row_length': min(len(row) for row in table_data['rows']) if table_data['rows'] else 0,
                'is_uniform': len(set(len(row) for row in table_data['rows'])) <= 1 if table_data['rows'] else True
            }
            
        except Exception as e:
            self.logger.warning(f"Error en extracción avanzada de tabla: {e}")
        
        return table_data
        
    def extract_stat_data(self, panel):
        """Extrae datos de estadísticas/métricas con detección avanzada"""
        stat_data = {
            'values': [],
            'labels': [],
            'units': [],
            'trends': [],
            'colors': [],
            'stat_type': 'unknown',
            'formatted_values': []
        }
        
        try:
            # === EXTRACCIÓN DE VALORES PRINCIPALES ===
            value_selectors = [
                '[style*="font-size"] span',
                '.css-1w5pd0q',
                '.stat-panel-value',
                '.single-stat-value',
                '.value',
                '.metric-value',
                '[class*="value"]',
                'span[style*="font-weight"]'
            ]
            
            for selector in value_selectors:
                value_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                for element in value_elements:
                    text = element.text.strip()
                    if text and len(text) > 0:
                        # Verificar si contiene números
                        if any(c.isdigit() for c in text):
                            if text not in stat_data['values']:
                                stat_data['values'].append(text)
                                
                                # Extraer información de estilo para detectar importancia
                                style = element.get_attribute('style') or ''
                                if 'font-size' in style:
                                    stat_data['formatted_values'].append({
                                        'value': text,
                                        'style': style,
                                        'element_class': element.get_attribute('class')
                                    })
            
            # === EXTRACCIÓN DE ETIQUETAS Y CONTEXTO ===
            label_selectors = [
                '.stat-panel-title',
                '.panel-title',
                '.metric-label',
                '.label',
                '[class*="label"]',
                'h1, h2, h3, h4, h5, h6',
                '.title'
            ]
            
            for selector in label_selectors:
                label_elements = panel.find_elements(By.CSS_SELECTOR, selector)
                for element in label_elements:
                    text = element.text.strip()
                    if text and text not in stat_data['labels'] and len(text) < 100:
                        # Evitar valores numéricos en las etiquetas
                        if not text.replace('.', '').replace(',', '').isdigit():
                            stat_data['labels'].append(text)
            
            # === DETECCIÓN DE UNIDADES ===
            # Buscar unidades comunes en el texto
            common_units = ['%', 'MB', 'GB', 'TB', 'KB', 'ms', 's', 'min', 'h', 'Hz', 'kHz', 'MHz', 'GHz', '$', '€', '£']
            
            for value in stat_data['values']:
                for unit in common_units:
                    if unit in value:
                        if unit not in stat_data['units']:
                            stat_data['units'].append(unit)
            
            # === DETECCIÓN DE TENDENCIAS ===
            trend_indicators = ['▲', '▼', '↑', '↓', '+', '-', 'up', 'down', 'increase', 'decrease']
            panel_text = panel.text.lower()
            
            for indicator in trend_indicators:
                if indicator.lower() in panel_text:
                    stat_data['trends'].append(indicator)
            
            # === DETECCIÓN DE COLORES (PARA ESTADOS) ===
            color_elements = panel.find_elements(By.CSS_SELECTOR, '[style*="color"], [class*="color"]')
            for element in color_elements:
                style = element.get_attribute('style') or ''
                class_name = element.get_attribute('class') or ''
                
                # Detectar colores comunes de estado
                if any(color in style.lower() or color in class_name.lower() 
                       for color in ['red', 'green', 'yellow', 'orange', 'blue']):
                    color_info = {
                        'element_text': element.text.strip(),
                        'style': style,
                        'class': class_name
                    }
                    stat_data['colors'].append(color_info)
            
            # === DETECCIÓN DE TIPO DE ESTADÍSTICA ===
            panel_html = panel.get_attribute('outerHTML').lower()
            
            if 'percentage' in panel_html or '%' in panel.text:
                stat_data['stat_type'] = 'percentage'
            elif 'count' in panel_html or any(val.isdigit() for val in stat_data['values']):
                stat_data['stat_type'] = 'count'
            elif any(unit in ['MB', 'GB', 'TB'] for unit in stat_data['units']):
                stat_data['stat_type'] = 'size'
            elif any(unit in ['ms', 's', 'min', 'h'] for unit in stat_data['units']):
                stat_data['stat_type'] = 'time'
            elif any(unit in ['$', '€', '£'] for unit in stat_data['units']):
                stat_data['stat_type'] = 'currency'
            else:
                stat_data['stat_type'] = 'metric'
            
            # === LIMPIAR DATOS DUPLICADOS ===
            stat_data['values'] = list(dict.fromkeys(stat_data['values']))  # Remover duplicados manteniendo orden
            stat_data['labels'] = list(dict.fromkeys(stat_data['labels']))
            stat_data['units'] = list(dict.fromkeys(stat_data['units']))
            
        except Exception as e:
            self.logger.warning(f"Error en extracción avanzada de estadísticas: {e}")
        
        return stat_data
        
    def extract_numeric_metrics(self, dashboard_data):
        """Extrae métricas numéricas visibles en el dashboard"""
        try:
            # Buscar elementos con números grandes (probablemente métricas)
            metric_selectors = [
                '[style*="font-size"] span',
                '.css-1w5pd0q',
                '[role="cell"]'
            ]
            
            metrics = {}
            for selector in metric_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    # Si es un número o contiene números significativos
                    if text and (text.isdigit() or (len(text) < 20 and any(c.isdigit() for c in text))):
                        # Buscar contexto (título del panel padre)
                        try:
                            parent_panel = element.find_element(By.XPATH, './ancestor::*[contains(@class, "react-grid-item") or contains(@class, "panel")]')
                            title_element = parent_panel.find_elements(By.CSS_SELECTOR, '.css-1gy0560-panel-title, h6')
                            context = title_element[0].text.strip() if title_element else 'unknown'
                            
                            if context not in metrics:
                                metrics[context] = []
                            metrics[context].append(text)
                        except:
                            if 'general' not in metrics:
                                metrics['general'] = []
                            metrics['general'].append(text)
            
            dashboard_data['metrics'] = metrics
            
        except Exception as e:
            self.logger.warning(f"Error extrayendo métricas numéricas: {e}")
            
    def extract_tables(self, dashboard_data):
        """Extrae todas las tablas del dashboard"""
        try:
            tables = self.driver.find_elements(By.CSS_SELECTOR, '[role="table"]')
            
            for i, table in enumerate(tables):
                table_data = {
                    'index': i,
                    'headers': [],
                    'rows': []
                }
                
                # Headers
                headers = table.find_elements(By.CSS_SELECTOR, '[role="columnheader"]')
                for header in headers:
                    text = header.text.strip()
                    if text:
                        table_data['headers'].append(text)
                
                # Rows
                rows = table.find_elements(By.CSS_SELECTOR, '[role="row"]:not([aria-label="table header"])')
                for row in rows:
                    cells = row.find_elements(By.CSS_SELECTOR, '[role="cell"]')
                    row_data = []
                    for cell in cells:
                        cell_text = cell.text.strip()
                        if cell_text:
                            row_data.append(cell_text)
                    if row_data:
                        table_data['rows'].append(row_data)
                
                if table_data['headers'] or table_data['rows']:
                    dashboard_data['tables'].append(table_data)
                    
        except Exception as e:
            self.logger.warning(f"Error extrayendo tablas: {e}")
            
    def save_dashboard_data(self, dashboard_data):
        """Guarda los datos del dashboard en archivos JSON apenas termina de procesarlo"""
        if not dashboard_data:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Usar dashboard_title (nueva estructura) o title (estructura antigua) como fallback
        title = dashboard_data.get('dashboard_title') or dashboard_data.get('title', 'unknown_dashboard')
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_title = safe_title.replace(' ', '_')[:50]  # Limitar longitud
        
        # Guardar JSON completo con datos reales
        json_filename = f"{self.output_dir}/{safe_title}_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"✅ Datos guardados para {title}: {json_filename}")
        
        # Log resumen de lo que se guardó
        availability_count = len(dashboard_data.get('availability_data', {}))
        charts_count = len(dashboard_data.get('charts_data', {}))
        tables_count = len(dashboard_data.get('tables_data', {}))
        
        if availability_count > 0 or charts_count > 0 or tables_count > 0:
            self.logger.info(f"   📊 Contenido: {availability_count} métricas, {charts_count} gráficos, {tables_count} tablas")
        else:
            self.logger.info("   ⚠️ Dashboard sin datos útiles extraídos")
        
    def run(self):
        """Ejecuta el proceso completo de scraping"""
        try:
            self.setup_driver()
            
            # Verificar si ya hay una sesión activa
            self.logger.info("Verificando si ya hay una sesión activa...")
            self.driver.get(self.dashboards_url)
            self.human_delay(3, 5)
            
            # Si ya estamos autenticados, saltar el login
            if "/login" not in self.driver.current_url and ("dashboard" in self.driver.current_url.lower() or 
                self.driver.find_elements(By.CSS_SELECTOR, '[data-testid*="Search section"]')):
                self.logger.info("✅ Sesión ya activa, saltando login SSO")
            else:
                # Login
                if not self.login_sso():
                    self.logger.error("❌ Fallo en login SSO")
                    return
            
            self.human_delay(2, 4)
            
            # Extraer URLs de dashboards
            dashboard_urls = self.extract_dashboard_urls()
            if not dashboard_urls:
                self.logger.error("No se encontraron dashboards")
                return
            
            # Procesar cada dashboard
            for i, dashboard_info in enumerate(dashboard_urls):
                self.logger.info(f"Procesando dashboard {i+1}/{len(dashboard_urls)}: {dashboard_info['title']}")
                
                try:
                    dashboard_data = self.extract_dashboard_data(dashboard_info)
                    if dashboard_data:
                        self.save_dashboard_data(dashboard_data)
                    
                    # Delay entre dashboards para parecer humano
                    if i < len(dashboard_urls) - 1:  # No delay después del último
                        self.human_delay(5, 10)
                        
                except Exception as e:
                    self.logger.error(f"Error procesando dashboard {dashboard_info['title']}: {e}")
                    continue
            
            self.logger.info("Scraping completado exitosamente")
            
        except Exception as e:
            self.logger.error(f"Error general en el scraping: {e}")
        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    scraper = GrafanaScraper()
    scraper.run()